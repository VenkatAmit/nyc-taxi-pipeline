{{ config(materialized='table') }}

SELECT  -- noqa: ST06
    TO_CHAR(hour_bucket, 'Month') AS trip_month_name,
    TO_CHAR(hour_bucket, 'Day') AS day_of_week,
    (
        TO_CHAR(hour_bucket, 'YYYYMMDD')
        || LPAD(EXTRACT(HOUR FROM hour_bucket)::TEXT, 2, '0')
    )::BIGINT AS date_key,
    DATE_TRUNC('day', hour_bucket)::DATE AS trip_date,
    EXTRACT(YEAR FROM hour_bucket)::INT AS trip_year,
    EXTRACT(MONTH FROM hour_bucket)::INT AS trip_month_num,
    EXTRACT(DAY FROM hour_bucket)::INT AS trip_day,
    EXTRACT(HOUR FROM hour_bucket)::INT AS trip_hour,
    EXTRACT(DOW FROM hour_bucket)::INT AS day_of_week_num,  -- noqa: RF02
    COALESCE(EXTRACT(DOW FROM hour_bucket) IN (0, 6), FALSE) AS is_weekend,
    CASE
        WHEN EXTRACT(HOUR FROM hour_bucket) BETWEEN 7 AND 9
            THEN 'morning_peak'
        WHEN EXTRACT(HOUR FROM hour_bucket) BETWEEN 17 AND 19
            THEN 'evening_peak'
        WHEN EXTRACT(HOUR FROM hour_bucket) BETWEEN 0 AND 5
            THEN 'overnight'
        ELSE 'off_peak'
    END AS time_of_day_bucket

FROM (
    SELECT DISTINCT DATE_TRUNC('hour', pickup_datetime) AS hour_bucket
    FROM {{ ref('silver_trips') }}
    WHERE pickup_datetime IS NOT NULL
) AS hours
ORDER BY date_key
