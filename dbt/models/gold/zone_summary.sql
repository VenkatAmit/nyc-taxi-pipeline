-- models/gold/zone_summary.sql
select
    pu_location_id,
    trip_month,
    count(*) as trip_count,
    round(avg(trip_duration_min)::numeric, 2) as avg_duration_min,
    round(avg(speed_mph)::numeric, 2) as avg_speed_mph,
    round(avg(fare_amount)::numeric, 2) as avg_fare_usd,
    round(avg(tip_percentage)::numeric, 2) as avg_tip_pct,
    round(sum(total_amount)::numeric, 2) as total_revenue_usd,
    sum(case when is_airport_trip then 1 else 0 end) as airport_trips,
    count(distinct date_trunc('day', pickup_datetime)) as active_days
from {{ source('public', 'cleaned_trips') }}
group by 1, 2
