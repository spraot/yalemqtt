FROM python:3.11-slim-bullseye

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update \
    & apt-get -y upgrade

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY src/* ./

CMD [ "python", "mqtt_heat.py", "/config.yml" ]
