{{ config(materialized='table') }}

SELECT
    location_id AS zone_key,
    TRIM(zone) AS zone_name,
    TRIM(borough) AS borough,
    TRIM(service_zone) AS service_zone,
    TRIM(borough) = 'Manhattan' AS is_manhattan
FROM {{ ref('taxi_zones') }}
ORDER BY zone_key
