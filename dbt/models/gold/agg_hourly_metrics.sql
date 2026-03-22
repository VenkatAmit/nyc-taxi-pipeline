{{ config(materialized='table') }}

SELECT
    d.date_key,
    d.trip_date,
    d.trip_year,
    d.trip_month_num,
    d.trip_month_name,
    d.trip_hour,
    d.day_of_week,
    d.is_weekend,
    d.time_of_day_bucket,
    pz.borough AS pickup_borough,
    pz.service_zone AS pickup_service_zone,
    COUNT(*) AS trip_count,
    ROUND(AVG(f.trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(f.fare_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(f.tip_amount)::NUMERIC, 2) AS avg_tip,
    ROUND(AVG(f.tip_pct)::NUMERIC, 2) AS avg_tip_pct,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(f.trip_duration_min)::NUMERIC, 2) AS avg_duration_min,
    ROUND(AVG(f.fare_per_mile)::NUMERIC, 2) AS avg_fare_per_mile

FROM {{ ref('fact_trips') }} AS f
INNER JOIN {{ ref('dim_date') }} AS d ON f.date_key = d.date_key
INNER JOIN {{ ref('dim_zone') }} AS pz ON f.pickup_zone_key = pz.zone_key

GROUP BY
    d.date_key,
    d.trip_date,
    d.trip_year,
    d.trip_month_num,
    d.trip_month_name,
    d.trip_hour,
    d.day_of_week,
    d.is_weekend,
    d.time_of_day_bucket,
    pz.borough,
    pz.service_zone
ORDER BY d.date_key, pz.borough
