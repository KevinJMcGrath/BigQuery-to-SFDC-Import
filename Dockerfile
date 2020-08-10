# Heroku Dockerfile Sample
# https://github.com/heroku/alpinehelloworld

FROM alpine:latest

RUN apk add --no-cache --update python3 py3-pip bash
ADD ./requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -q -r /tmp/requirements.txt

ADD . /opt/bqimport/
WORKDIR /opt/bqimport

RUN adduser -D kevin_as
USER kevin_as

CMD python3 main.py