{{ config(materialized='table') }}

SELECT
    "LocationID" AS zone_key,  -- noqa: RF06
    TRIM("Zone") AS zone_name,  -- noqa: RF06
    TRIM("Borough") AS borough,  -- noqa: RF06
    TRIM(service_zone) AS service_zone,
    TRIM("Borough") = 'Manhattan' AS is_manhattan  -- noqa: RF06
FROM {{ ref('taxi_zones') }}
ORDER BY zone_key
