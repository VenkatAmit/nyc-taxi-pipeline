FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.3.0 \
    delta-spark==2.2.0 \
    dbt-postgres==1.10.0 \
    great-expectations==0.18.19 \
    sqlalchemy==1.4.52
