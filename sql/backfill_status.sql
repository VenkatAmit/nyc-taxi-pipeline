-- NYC Taxi Pipeline — Backfill Progress Monitor
-- Usage: psql -h localhost -p 5433 -U pipeline -d nyc_taxi -f sql/backfill_status.sql
-- Run this while the backfill is in progress to track all 23 monthly runs

-- Per-run detail
SELECT
    trip_month,
    CASE WHEN quality_passed THEN 'PASS' ELSE 'FAIL' END   AS quality,
    rows_ingested,
    rows_cleaned,
    rows_ingested - rows_cleaned                            AS rows_dropped,
    ROUND(total_duration_sec / 60.0, 1)                    AS duration_min,
    TO_CHAR(run_completed_at, 'YYYY-MM-DD HH24:MI')        AS completed_at
FROM pipeline_run_log
ORDER BY trip_month;

-- Summary
SELECT
    COUNT(*)                                                AS runs_completed,
    COUNT(*) FILTER (WHERE quality_passed)                  AS passed,
    COUNT(*) FILTER (WHERE NOT quality_passed)              AS failed,
    23 - COUNT(*)                                           AS remaining,
    ROUND(SUM(total_duration_sec) / 3600.0, 1)             AS total_hours,
    ROUND(AVG(total_duration_sec) / 60.0, 1)               AS avg_min_per_run
FROM pipeline_run_log;
