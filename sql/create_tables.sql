-- ============================================================
-- NYC Taxi Pipeline - Medallion Schema
-- bronze -> silver (dbt) -> gold (dbt)
-- ============================================================

-- ── Bronze: raw_trips ────────────────────────────────────────
-- Exact copy of TLC parquet columns. No transformation.
-- Append-only. Partitioned by pickup month for query efficiency.

CREATE TABLE IF NOT EXISTS raw_trips (
    id                      BIGSERIAL PRIMARY KEY,
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMPTZ,
    dropoff_datetime        TIMESTAMPTZ,
    passenger_count         SMALLINT,
    trip_distance           NUMERIC(8, 2),
    rate_code_id            SMALLINT,
    store_and_fwd_flag      CHAR(1),
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            SMALLINT,
    fare_amount             NUMERIC(8, 2),
    extra                   NUMERIC(8, 2),
    mta_tax                 NUMERIC(8, 2),
    tip_amount              NUMERIC(8, 2),
    tolls_amount            NUMERIC(8, 2),
    improvement_surcharge   NUMERIC(8, 2),
    total_amount            NUMERIC(8, 2),
    congestion_surcharge    NUMERIC(8, 2),
    airport_fee             NUMERIC(8, 2),
    -- Pipeline metadata
    trip_month              CHAR(7)      NOT NULL,  -- YYYY-MM
    pipeline_run_id         VARCHAR(100) NOT NULL,
    ingested_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_pickup_datetime
    ON raw_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_raw_trip_month
    ON raw_trips (trip_month);
CREATE INDEX IF NOT EXISTS idx_raw_pipeline_run
    ON raw_trips (pipeline_run_id);


-- ── Silver + Gold tables created by dbt ──────────────────────
-- dbt models write to:
--   cleaned_trips   (silver)
--   trip_metrics    (gold)
--   zone_summary    (gold)
-- dbt handles CREATE TABLE / INSERT for these layers.


-- ── Pipeline observability ────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id                      BIGSERIAL PRIMARY KEY,
    pipeline_run_id         VARCHAR(100)    NOT NULL,
    dag_id                  VARCHAR(100)    NOT NULL,
    trip_month              CHAR(7)         NOT NULL,
    run_started_at          TIMESTAMPTZ     NOT NULL,
    run_completed_at        TIMESTAMPTZ,

    -- Row counts per layer
    rows_ingested           INTEGER,
    rows_cleaned            INTEGER,
    rows_transformed        INTEGER,

    -- Data quality
    quality_passed          BOOLEAN,
    quality_notes           TEXT,

    -- Task timing
    ingest_duration_sec     NUMERIC(8, 2),
    spark_duration_sec      NUMERIC(8, 2),
    dbt_duration_sec        NUMERIC(8, 2),
    total_duration_sec      NUMERIC(8, 2),

    CONSTRAINT uq_pipeline_run UNIQUE (pipeline_run_id)
);
