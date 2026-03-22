{{ config(materialized='table') }}

SELECT
    pz.zone_key AS pickup_zone_key,
    pz.zone_name AS pickup_zone_name,
    pz.borough AS pickup_borough,
    pz.service_zone,
    pz.is_manhattan,
    f.trip_month,
    COUNT(*) AS trip_count,
    ROUND(AVG(f.trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(f.fare_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(f.tip_pct)::NUMERIC, 2) AS avg_tip_pct,
    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue,
    COUNT(DISTINCT f.dropoff_zone_key) AS unique_destinations

FROM {{ ref('fact_trips') }} AS f
INNER JOIN {{ ref('dim_zone') }} AS pz ON f.pickup_zone_key = pz.zone_key

GROUP BY
    pz.zone_key,
    pz.zone_name,
    pz.borough,
    pz.service_zone,
    pz.is_manhattan,
    f.trip_month
ORDER BY trip_count DESC
