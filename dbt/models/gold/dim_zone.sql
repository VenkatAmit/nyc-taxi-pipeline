{{ config(materialized='table') }}

SELECT
    locationid AS zone_key,
    locationid AS location_id,
    TRIM(zone) AS zone_name,
    TRIM(borough) AS borough,
    TRIM(service_zone) AS service_zone,
    TRIM(borough) = 'Manhattan' AS is_manhattan
FROM {{ ref('taxi_zones') }}
ORDER BY zone_key
