# Heroku Dockerfile Sample
# https://github.com/heroku/alpinehelloworld

# FROM alpine:latest
FROM python:3.8-buster

# RUN apk add --no-cache --update python3 py3-pip bash
ADD ./requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -q -r /tmp/requirements.txt

ADD . /opt/bqimport/
WORKDIR /opt/bqimport

RUN adduser --disabled-password kevin_as
USER kevin_as

CMD python3 main.py -j