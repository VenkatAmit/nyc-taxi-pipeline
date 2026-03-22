{{ config(materialized='table') }}

SELECT  -- noqa: ST06
    TO_CHAR(pickup_datetime, 'Month') AS trip_month_name,
    TO_CHAR(pickup_datetime, 'Day') AS day_of_week,
    (
        TO_CHAR(pickup_datetime, 'YYYYMMDD')
        || LPAD(EXTRACT(HOUR FROM pickup_datetime)::TEXT, 2, '0')
    )::BIGINT AS date_key,
    DATE_TRUNC('day', pickup_datetime)::DATE AS trip_date,
    EXTRACT(YEAR FROM pickup_datetime)::INT AS trip_year,
    EXTRACT(MONTH FROM pickup_datetime)::INT AS trip_month_num,
    EXTRACT(DAY FROM pickup_datetime)::INT AS trip_day,
    EXTRACT(HOUR FROM pickup_datetime)::INT AS trip_hour,
    EXTRACT(DOW FROM pickup_datetime)::INT AS day_of_week_num,
    COALESCE(EXTRACT(DOW FROM pickup_datetime) IN (0, 6), FALSE) AS is_weekend,
    CASE
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 7 AND 9
            THEN 'morning_peak'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 17 AND 19
            THEN 'evening_peak'
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 0 AND 5
            THEN 'overnight'
        ELSE 'off_peak'
    END AS time_of_day_bucket

FROM {{ source('silver', 'cleaned_trips') }}
WHERE pickup_datetime IS NOT NULL
GROUP BY pickup_datetime
ORDER BY date_key
