FROM grafana/grafana-oss:latest

USER root

RUN rm -rf /var/lib/grafana/plugins/*

COPY mongodb-plugin.zip /tmp/plugin1.zip
COPY meln5674-mongodb-community.zip /tmp/plugin2.zip

RUN unzip /tmp/plugin1.zip -d /var/lib/grafana/plugins/ && \
    mkdir -p /var/lib/grafana/plugins/meln5674-mongodb-community && \
    unzip /tmp/plugin2.zip -d /var/lib/grafana/plugins/meln5674-mongodb-community/ && \
    chown -R 472:472 /var/lib/grafana/plugins && \
    rm /tmp/plugin1.zip /tmp/plugin2.zip

USER grafana

ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=ludovicm67-mongodb-datasource,meln5674-mongodb-community