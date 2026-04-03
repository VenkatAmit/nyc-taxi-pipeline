{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_trips') }}
    {% if is_incremental() %}
        WHERE partition_date > ( -- noqa: RF02
            SELECT
                COALESCE(
                    MAX(partition_date), -- noqa: RF02
                    CAST('1900-01-01' AS date)
                )
            FROM {{ this }}
        )
    {% endif %}
),

enriched AS (
    SELECT
        vendor_id,
        pu_location_id,
        do_location_id,
        partition_date,
        pickup_datetime,
        dropoff_datetime,
        pickup_hour,
        pickup_day_of_week,
        pickup_hour_of_day,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        fare_amount,
        tip_amount,
        total_amount,
        ingested_at,
        MD5(
            COALESCE(CAST(vendor_id AS varchar), '_null_')
            || '|'
            || COALESCE(CAST(pickup_datetime AS varchar), '_null_')
            || '|'
            || COALESCE(CAST(pu_location_id AS varchar), '_null_')
        ) AS trip_id,
        CASE
            WHEN trip_distance > 0
                THEN ROUND(CAST(fare_amount / trip_distance AS numeric), 2)
        END AS fare_per_mile,
        CASE
            WHEN total_amount > 0
                THEN ROUND(CAST(tip_amount / total_amount * 100 AS numeric), 2)
            ELSE 0
        END AS tip_pct,
        tip_amount > 0 AS has_tip,
        CURRENT_TIMESTAMP AS transformed_at
    FROM source
)

SELECT * FROM enriched
