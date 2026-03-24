{{ config(materialized='table') }}

WITH trips AS (
    SELECT
        *,
        (
            TO_CHAR(pickup_datetime, 'YYYYMMDD')
            || LPAD(EXTRACT(HOUR FROM pickup_datetime)::TEXT, 2, '0')
        )::BIGINT AS pickup_date_key
    FROM {{ source('silver', 'cleaned_trips') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
),

zone_dim AS (
    SELECT * FROM {{ ref('dim_zone') }}
),

vendor_dim AS (
    SELECT * FROM {{ ref('dim_vendor') }}
)

SELECT  -- noqa: ST06
    t.trip_month,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.passenger_count,
    COALESCE(t.pickup_date_key, -1) AS date_key,
    COALESCE(pz.zone_key, -1) AS pickup_zone_key,
    COALESCE(dz.zone_key, -1) AS dropoff_zone_key,
    COALESCE(v.vendor_key, 0) AS vendor_key,
    (
        EXTRACT(EPOCH FROM t.dropoff_datetime - t.pickup_datetime) -- noqa: RF02
    ) / 60.0 AS trip_duration_min,
    CASE
        WHEN t.trip_distance > 0
            THEN ROUND((t.fare_amount / t.trip_distance)::NUMERIC, 2)
    END AS fare_per_mile,
    CASE
        WHEN t.fare_amount > 0
            THEN ROUND((t.tip_amount / t.fare_amount * 100)::NUMERIC, 2)
        ELSE 0
    END AS tip_pct,
    ('x' || SUBSTR(MD5(
        t.pickup_datetime::TEXT || t.dropoff_datetime::TEXT
        || t.pu_location_id::TEXT || t.do_location_id::TEXT
        || t.fare_amount::TEXT || t.tip_amount::TEXT
    ), 1, 16))::BIT(64)::BIGINT AS trip_key

FROM trips AS t

LEFT JOIN date_dim AS d ON t.pickup_date_key = d.date_key

LEFT JOIN zone_dim AS pz ON t.pu_location_id = pz.zone_key
LEFT JOIN zone_dim AS dz ON t.do_location_id = dz.zone_key

LEFT JOIN vendor_dim AS v ON t.vendor_id::TEXT = v.vendor_id
