"""
airflow/tasks/spark_transform.py
---------------------------------
Silver layer: reads raw_trips from Postgres, applies cleaning
and enrichment using PySpark, writes to cleaned_trips.

Why PySpark instead of pandas?
- raw_trips has 3.3M rows per month — pandas loads all into memory
- PySpark processes in parallel across all available CPU cores
- Same code scales to 333M rows on a multi-node cluster unchanged
- Demonstrates the tool that 80% of Indian DE job specs require

Cleaning rules applied:
1. Filter invalid pickup dates (TLC data has erroneous 2002 timestamps)
2. Cap fare_amount to realistic range (0-500 USD)
3. Cap trip_distance to realistic range (0.01-200 miles)
4. Validate payment_type against TLC standard codes (1-6)

Enrichment columns added:
- trip_duration_min: pickup to dropoff in minutes
- speed_mph: distance / duration
- is_airport_trip: pickup or dropoff at JFK/LGA/EWR
- tip_percentage: tip / fare * 100

XCom output:
    rows_cleaned (int)
    rows_dropped (int)
    spark_duration_sec (float)
"""

import os
import time
import logging

log = logging.getLogger(__name__)

# TLC location IDs for NYC airports
AIRPORT_LOCATION_IDS = {132, 138, 1}  # JFK, LGA, EWR

# Valid TLC payment type codes
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}


def get_spark_session():
    """
    Create a local Spark session.
    local[*] uses all available CPU cores.
    In production this would point to a remote cluster master.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("nyc_taxi_silver")
        .config("spark.driver.memory", "3g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.6.0",
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
    Read raw_trips for the current month from Postgres.
    Filters by trip_month at the DB level to avoid full table scan.
    """
    jdbc_url = get_jdbc_url()
    props = get_jdbc_props()

    query = f"""
        (SELECT * FROM raw_trips
         WHERE trip_month = '{trip_month}') AS bronze
    """

    log.info(f"Reading bronze layer for trip_month={trip_month}")
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    log.info(f"Bronze rows loaded: {df.count():,}")
    return df


def apply_cleaning_rules(df):
    """
    Apply data quality filters to remove invalid rows.
    Each filter is logged separately for observability.
    """
    from pyspark.sql import functions as F

    initial_count = df.count()

    # Rule 1: Valid pickup dates (TLC data has erroneous historical dates)
    df = df.filter(
        (F.col("pickup_datetime") >= "2009-01-01")
        & (F.col("pickup_datetime") < "2025-01-01")
    )
    log.info(f"After date filter: {df.count():,} rows")

    # Rule 2: Valid fare amount
    df = df.filter((F.col("fare_amount") > 0) & (F.col("fare_amount") <= 500))
    log.info(f"After fare filter: {df.count():,} rows")

    # Rule 3: Valid trip distance
    df = df.filter((F.col("trip_distance") > 0.01) & (F.col("trip_distance") <= 200))
    log.info(f"After distance filter: {df.count():,} rows")

    # Rule 4: Valid payment type
    df = df.filter(F.col("payment_type").isin(list(VALID_PAYMENT_TYPES)))
    log.info(f"After payment_type filter: {df.count():,} rows")

    final_count = df.count()
    rows_dropped = initial_count - final_count
    log.info(f"Cleaning complete: dropped {rows_dropped:,} rows")

    return df, rows_dropped


def add_derived_columns(df):
    """
    Add enrichment columns that make the silver layer useful
    for downstream analytics and ML features.
    """
    from pyspark.sql import functions as F

    # Trip duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
        / 60,
    )

    # Speed in mph — avoid division by zero
    df = df.withColumn(
        "speed_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.col("trip_distance") / (F.col("trip_duration_min") / 60),
        ).otherwise(None),
    )

    # Airport trip flag
    df = df.withColumn(
        "is_airport_trip",
        F.col("pu_location_id").isin(list(AIRPORT_LOCATION_IDS))
        | F.col("do_location_id").isin(list(AIRPORT_LOCATION_IDS)),
    )

    # Tip percentage
    df = df.withColumn(
        "tip_percentage",
        F.when(
            F.col("fare_amount") > 0,
            (F.col("tip_amount") / F.col("fare_amount")) * 100,
        ).otherwise(None),
    )

    # Pipeline metadata — use current_timestamp() so Spark writes
    # a native TimestampType, not a string. Postgres TIMESTAMPTZ requires this.
    df = df.withColumn("cleaned_at", F.current_timestamp())

    return df


def write_silver(df, trip_month: str):
    """
    Write cleaned data to cleaned_trips table.
    Selects only columns that exist in cleaned_trips schema —
    drops raw-only columns like ingested_at and pipeline_run_id
    from raw_trips that are not part of the silver layer.
    """
    from pyspark.sql import functions as F

    jdbc_url = get_jdbc_url()
    props = get_jdbc_props()

    # Idempotency: delete existing rows for this month before insert
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

    # Add trip_month column
    df = df.withColumn("trip_month", F.lit(trip_month))

    # Select only columns that exist in cleaned_trips
    # Explicitly excludes raw-only columns: id, ingested_at, pipeline_run_id
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

    # Only select columns that actually exist in the DataFrame
    available = [c for c in silver_columns if c in df.columns]
    df = df.select(available)

    row_count = df.count()
    log.info(f"Writing {row_count:,} rows to cleaned_trips in batches")

    # Write in batches to avoid JVM OOM on large datasets
    # Repartition controls how many parallel JDBC connections Spark opens
    WRITE_PARTITIONS = 8
    df = df.repartition(WRITE_PARTITIONS)

    df.write.option("batchsize", 10000).jdbc(
        url=jdbc_url,
        table="cleaned_trips",
        mode="append",
        properties=props,
    )

    log.info(f"Silver write complete: {row_count:,} rows")
    return row_count


def spark_transform(**context):
    """
    Main Spark transform task callable for Airflow PythonOperator.
    """
    start = time.time()
    ti = context["ti"]

    # Get trip_month from ingest task via XCom
    trip_month = ti.xcom_pull(task_ids="ingest", key="trip_month")
    if not trip_month:
        data_interval_start = context.get("data_interval_start")
        trip_month = data_interval_start.strftime("%Y-%m")

    log.info(f"Starting spark_transform | trip_month={trip_month}")

    spark = get_spark_session()

    try:
        # Read bronze
        df = read_bronze(spark, trip_month)
        raw_count = df.count()

        # Clean
        df, rows_dropped = apply_cleaning_rules(df)

        # Enrich
        df = add_derived_columns(df)

        # Write silver
        rows_cleaned = write_silver(df, trip_month)

    finally:
        spark.stop()
        log.info("Spark session stopped")

    duration = round(time.time() - start, 2)
    log.info(
        f"spark_transform complete | "
        f"raw={raw_count:,} | cleaned={rows_cleaned:,} | "
        f"dropped={rows_dropped:,} | duration={duration}s"
    )

    ti.xcom_push(key="rows_cleaned", value=rows_cleaned)
    ti.xcom_push(key="rows_dropped", value=rows_dropped)
    ti.xcom_push(key="spark_duration_sec", value=duration)

    return rows_cleaned
