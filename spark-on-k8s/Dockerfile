FROM openjdk:8-alpine

ARG spark_base
ARG spark_jars=${spark_base}/jars
ARG k8s_tests=${spark_base}/kubernetes/tests

RUN set -ex && \
    apk upgrade --no-cache && \
    apk add --no-cache bash tini libc6-compat linux-pam && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

COPY ${spark_base}/jars /opt/spark/jars
COPY ${spark_base}/bin /opt/spark/bin
COPY ${spark_base}/sbin /opt/spark/sbin
COPY entrypoint.sh /opt/
COPY ${spark_base}/examples /opt/spark/examples
COPY ${k8s_tests} /opt/spark/tests
COPY ${spark_base}/data /opt/spark/data
COPY dependencies /opt/spark/dependencies

# workaround for libnss3.so
RUN apk add --no-cache nss

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]
