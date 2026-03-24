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
    pyspark==3.4.1 \
    delta-spark==2.3.0 \
    dbt-postgres==1.7.18 \
    great-expectations==0.17.23 \
    sqlalchemy==1.4.52 \
    pandas==1.5.3 \
    pyarrow==14.0.1

RUN python - << 'PYEOF'
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
builder = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
configure_spark_with_delta_pip(builder).getOrCreate().stop()
PYEOF
