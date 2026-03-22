{{ config(materialized='table') }}

WITH vendors AS (
    SELECT
        1 AS vendor_key,
        '1' AS vendor_id,
        'Creative Mobile Technologies' AS vendor_name,
        'CMT' AS vendor_code
    UNION ALL
    SELECT
        2 AS vendor_key,
        '2' AS vendor_id,
        'VeriFone Inc.' AS vendor_name,
        'VTS' AS vendor_code
    UNION ALL
    SELECT
        0 AS vendor_key,
        'unknown' AS vendor_id,
        'Unknown Vendor' AS vendor_name,
        'UNK' AS vendor_code
)

SELECT
    vendor_key,
    vendor_id,
    vendor_name,
    vendor_code
FROM vendors
