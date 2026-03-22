"""
airflow/tasks/spark_transform.py
---------------------------------
Silver layer: reads bronze from Delta Lake, applies cleaning
and enrichment using PySpark, writes cleaned data to Postgres
cleaned_trips (silver stays in Postgres for dbt compatibility).

Why read Delta for bronze but write Postgres for silver?
- Bronze Delta: schema-flexible raw store with time travel
- Silver Postgres: dbt reads via JDBC; Delta-native dbt adapter
  is a future migration step once delta-spark is stable in prod

XCom output:
    rows_cleaned (int)
    rows_dropped (int)
    spark_duration_sec (float)
"""

import os
import time
import logging

log = logging.getLogger(__name__)

DELTA_BRONZE_PATH = os.environ.get(
    "DELTA_BRONZE_PATH", "/opt/airflow/data/delta/bronze/yellow_tripdata"
)

AIRPORT_LOCATION_IDS = {132, 138, 1}
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}


def get_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("nyc_taxi_silver")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.postgresql:postgresql:42.6.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def get_jdbc_url():
    host = os.environ["PIPELINE_DB_HOST"]
    port = os.environ["PIPELINE_DB_PORT"]
    db = os.environ["PIPELINE_DB_NAME"]
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_props():
    return {
        "user": os.environ["PIPELINE_DB_USER"],
        "password": os.environ["PIPELINE_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
    }


def read_bronze(spark, trip_month: str):
    """
    Read bronze Delta table filtered to the current trip_month.
    Delta partition pruning means only the relevant Parquet files
    are read — equivalent to the old WHERE trip_month = ? JDBC push-down.
    """
    log.info(f"Reading bronze Delta for trip_month={trip_month}")
    df = (
        spark.read.format("delta")
        .load(DELTA_BRONZE_PATH)
        .filter(f"trip_month = '{trip_month}'")
    )
    count = df.count()
    log.info(f"Bronze rows loaded: {count:,}")
    return df


def apply_cleaning_rules(df):
    from pyspark.sql import functions as F

    initial_count = df.count()

    df = df.filter(
        (F.col("tpep_pickup_datetime") >= "2009-01-01")
        & (F.col("tpep_pickup_datetime") < "2025-01-01")
    )
    log.info(f"After date filter: {df.count():,} rows")

    df = df.filter((F.col("fare_amount") > 0) & (F.col("fare_amount") <= 500))
    log.info(f"After fare filter: {df.count():,} rows")

    df = df.filter((F.col("trip_distance") > 0.01) & (F.col("trip_distance") <= 200))
    log.info(f"After distance filter: {df.count():,} rows")

    df = df.filter(
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60
        > 0
    )
    df = df.filter(
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60
        <= 300
    )
    log.info(f"After duration filter: {df.count():,} rows")

    df = df.filter(F.col("payment_type").isin(list(VALID_PAYMENT_TYPES)))
    log.info(f"After payment_type filter: {df.count():,} rows")

    final_count = df.count()
    rows_dropped = initial_count - final_count
    log.info(f"Cleaning complete: dropped {rows_dropped:,} rows")

    return df, rows_dropped


def add_derived_columns(df):
    from pyspark.sql import functions as F

    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    df = df.withColumnRenamed("VendorID", "vendor_id")
    df = df.withColumnRenamed("RatecodeID", "rate_code_id")
    df = df.withColumnRenamed("PULocationID", "pu_location_id")
    df = df.withColumnRenamed("DOLocationID", "do_location_id")

    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
        / 60,
    )
    df = df.withColumn(
        "speed_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.col("trip_distance") / (F.col("trip_duration_min") / 60),
        ).otherwise(None),
    )
    df = df.withColumn(
        "is_airport_trip",
        F.col("pu_location_id").isin(list(AIRPORT_LOCATION_IDS))
        | F.col("do_location_id").isin(list(AIRPORT_LOCATION_IDS)),
    )
    df = df.withColumn(
        "tip_percentage",
        F.when(
            F.col("fare_amount") > 0,
            (F.col("tip_amount") / F.col("fare_amount")) * 100,
        ).otherwise(None),
    )
    df = df.withColumn("cleaned_at", F.current_timestamp())

    return df


def write_silver(df, trip_month: str):
    from pyspark.sql import functions as F

    jdbc_url = get_jdbc_url()
    props = get_jdbc_props()

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["PIPELINE_DB_HOST"],
        port=os.environ["PIPELINE_DB_PORT"],
        dbname=os.environ["PIPELINE_DB_NAME"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cleaned_trips WHERE trip_month = %s", (trip_month,)
            )
            deleted = cur.rowcount
            conn.commit()
        if deleted > 0:
            log.warning(f"Deleted {deleted:,} existing silver rows for {trip_month}")
    finally:
        conn.close()

    df = df.withColumn("trip_month", F.lit(trip_month))

    silver_columns = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "trip_duration_min",
        "speed_mph",
        "is_airport_trip",
        "tip_percentage",
        "trip_month",
        "cleaned_at",
    ]

    available = [c for c in silver_columns if c in df.columns]
    df = df.select(available)

    row_count = df.count()
    log.info(f"Writing {row_count:,} rows to cleaned_trips")

    df.repartition(8).write.option("batchsize", 10000).jdbc(
        url=jdbc_url, table="cleaned_trips", mode="append", properties=props
    )

    log.info(f"Silver write complete: {row_count:,} rows")
    return row_count


def spark_transform(**context):
    start = time.time()
    ti = context["ti"]

    trip_month = ti.xcom_pull(task_ids="ingest", key="trip_month")
    if not trip_month:
        trip_month = context.get("data_interval_start").strftime("%Y-%m")

    log.info(f"Starting spark_transform | trip_month={trip_month}")

    spark = get_spark_session()

    try:
        df = read_bronze(spark, trip_month)
        raw_count = df.count()
        df, rows_dropped = apply_cleaning_rules(df)
        df = add_derived_columns(df)
        rows_cleaned = write_silver(df, trip_month)
    finally:
        spark.stop()
        log.info("Spark session stopped")

    duration = round(time.time() - start, 2)
    log.info(
        f"spark_transform complete | raw={raw_count:,} | "
        f"cleaned={rows_cleaned:,} | dropped={rows_dropped:,} | duration={duration}s"
    )

    ti.xcom_push(key="rows_cleaned", value=rows_cleaned)
    ti.xcom_push(key="rows_dropped", value=rows_dropped)
    ti.xcom_push(key="spark_duration_sec", value=duration)

    return rows_cleaned
