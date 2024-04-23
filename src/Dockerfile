FROM python:3.12

RUN pip install requests \
    pip install feedparser \
    pip install dynaconf \
    pip install elasticsearch \
    pip install psycopg2

WORKDIR /rss_scanner
RUN mkdir -p ./logs

ADD settings.yaml settings.yaml
ADD .secrets.yaml .secrets.yaml
ADD main.py main.py
