FROM python:3.9.7-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y wget software-properties-common lsb-release gcc make python3 python3-pip python3-dev libsasl2-modules-gssapi-mit && \
    apt-get -yqq install krb5-user libpam-krb5 && \
    apt-get -yqq clean && \
    wget -qO - https://packages.confluent.io/deb/7.0/archive.key | apt-key add - && \
    add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main" && \
    apt-get install -y librdkafka-dev openjdk-11-jre-headless && \
    mkdir /etl

WORKDIR /etl

COPY ./src /etl/src

COPY ./requirements* /etl

RUN pip3 install  --requirement ./requirements.txt
