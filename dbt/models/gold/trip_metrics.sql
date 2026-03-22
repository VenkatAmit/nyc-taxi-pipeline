-- models/gold/trip_metrics.sql
select
    trip_month,
    date_trunc('hour', pickup_datetime) as pickup_hour,
    count(*) as trip_count,
    round(avg(fare_amount)::numeric, 2) as avg_fare_usd,
    round(avg(tip_percentage)::numeric, 2) as avg_tip_pct,
    round(avg(trip_duration_min)::numeric, 2) as avg_duration_min,
    round(avg(speed_mph)::numeric, 2) as avg_speed_mph,
    round(sum(total_amount)::numeric, 2) as total_revenue_usd,
    sum(case when is_airport_trip then 1 else 0 end) as airport_trip_count,
    sum(case when payment_type = 1 then 1 else 0 end) as card_payments,
    sum(case when payment_type = 2 then 1 else 0 end) as cash_payments
from {{ source('public', 'cleaned_trips') }}
group by 1, 2
