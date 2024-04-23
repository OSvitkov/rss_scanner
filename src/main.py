import random
import threading
import os
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import requests
import feedparser
from dynaconf import Dynaconf
import logging
from elasticsearch import Elasticsearch
import psycopg2

settings = Dynaconf(
    settings_files=['./settings.yaml', './.secrets.yaml'],
)

shutdown = False

# Keywords to filter of content;
key_words = list(map(lambda w: w.lower(), settings.key.words))

es = Elasticsearch([{'host': settings.elasticsearch.host,
                     'port': settings.elasticsearch.port,
                     'scheme': settings.elasticsearch.scheme}],
                   basic_auth=(settings.elasticsearch.user, settings.esearch.passwd),
                   connections_per_node=settings.elasticsearch.connections_per_node,
                   )

logger = logging.getLogger(__name__)
logging.basicConfig(filename=settings.logging.path, filemode='w', encoding='utf-8',
                    level=logging.getLevelName(settings.logging.level),
                    format='%(asctime)s %(message)s', datefmt='%m.%d.%Y %I:%M:%S %p')
logging.getLogger().addHandler(logging.StreamHandler())


def init_db_pg():
    global shutdown
    global conn_pg
    try:
        conn_pg = psycopg2.connect(dbname=settings.postgresql.dbname,
                                   user=settings.postgresql.user,
                                   password=settings.pgsql.passwd,
                                   host=settings.postgresql.host,
                                   port=settings.postgresql.port)

        with conn_pg.cursor() as cur:
            cur.execute('CREATE TABLE IF NOT EXISTS rss_news'
                        ' (id serial PRIMARY KEY,'
                        ' title text,'
                        ' summary text,'
                        ' src_link text,'
                        ' rss_link text,'
                        ' published  TIMESTAMP WITH TIME ZONE,'
                        ' date_create TIMESTAMP WITH TIME ZONE);'
                        'CREATE INDEX IF NOT EXISTS rss_news_date_create_indx ON rss_news (date_create);'
                        'DELETE FROM rss_news where date_create > CURRENT_DATE-1;')
            conn_pg.commit()
    except Exception as e:
        logging.error(f'Can`t establish connection to Postgresql [{e}]')
        shutdown = True


def save_pg(_title, _summary, _src_link, _rss_link, _published):
    global conn_pg
    try:
        with conn_pg.cursor() as cur:
            cur.execute("INSERT INTO rss_news (title, summary, src_link, rss_link, published, date_create)"
                        " VALUES (%s, %s, %s, %s, %s, %s)",
                        (_title, _summary, _src_link, _rss_link, _published, datetime.now()))
            conn_pg.commit()
    except Exception as e:
        logging.error(f'Can`t save data to Postgresql [{e}]')


def filters(_src, _pubdate, _title, _summary):
    # Formate date obtained from RSS:
    rss2datetime = parsedate_to_datetime(_pubdate).replace(tzinfo=None)

    if rss2datetime > rss_filters.get(_src):
        if filter_corpus_words(_title + ' ' + _summary):
            rss_filters[_src] = rss2datetime
            return False

    return True


def filter_corpus_words(_content):
    global key_words
    words = _content.replace('?', ' ').replace('.', ' ').replace('\n', ' ').replace('"', '').lower().split()

    for key in key_words:
        if key in words:
            return True

    return False


def exit_handler(_signum, _frame):
    global shutdown
    logger.info(f'Shutdown.... Signum: [{_signum}] Frame: [{_frame}]')
    shutdown = True


def task_rss(_src: str, _rss_link: str):
    try:
        logger.debug(f'Thread: [{threading.current_thread().native_id}]; [{_rss_link}]')

        resp = requests.get(_rss_link)
        if resp.status_code != 200:
            return

        feed = feedparser.parse(resp.content)

        for entry in feed.entries[:50][::-1]:
            summary = entry['summary'] if 'summary' in entry else ''
            title = entry['title'] if 'title' in entry else ''
            pubdate = entry['published'] if 'published' in entry else ''
            link = entry['link'] if 'link' in entry else ''

            if filters(_src, pubdate, title, summary):
                continue

            result_text = f'{title}\n{summary}\n{pubdate}'
            post = f'<b>{_src}</b>\n{link}\n{result_text}'
            logger.debug(post)

            if settings.postgresql.enabled:
                save_pg(title, summary, _src, link, pubdate)

    except Exception as e:
        logger.error(f"Exception: [{e}]")
    logger.debug(f'Finish scan: [{_src}] Thread: [{threading.current_thread().native_id}]')
    return


if __name__ == '__main__':
    signal.signal(signal.SIGINT, exit_handler)
    signal.signal(signal.SIGTERM, exit_handler)

    logger.info(
        f"Process ID: [{os.getpid()}]; Thread workers: [{settings.pool.thread.workers}]")

    if settings.postgresql.enabled:
        logger.info('Check & init of Postgresql')
        init_db_pg()

    # Sites to scan;
    rss_sites = settings.rss.sites
    rss_filters = rss_sites.copy()
    rss_filters.update((key, (datetime.now() - timedelta(days=1))) for key in rss_sites)

    with ThreadPoolExecutor(max_workers=settings.pool.thread.workers) as thread_exec:
        while not shutdown:
            for src, rss_link in rss_sites.items():
                if shutdown is False:
                    thread_exec.submit(task_rss, src, rss_link)
                    sleep(random.uniform(0.1, 0.7))
            sleep(random.uniform(settings.pool.thread.sleep * 3 / 4, settings.pool.thread.sleep))

    thread_exec.shutdown()
