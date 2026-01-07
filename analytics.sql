-- analytics.sql
-- analytical sql queries for urban mobility insights

-- peak demand hours
select
    pickup_hour,
    count(*) as trip_count
from taxi_trips
group by pickup_hour
order by trip_count desc;

-- revenue by pickup zone (coordinates as proxy)
select
    pickup_longitude,
    pickup_latitude,
    sum(total_amount) as total_revenue,
    count(*) as trip_count
from taxi_trips
group by pickup_longitude, pickup_latitude
order by total_revenue desc
limit 20;

-- top 10 highest revenue days
select
    date(tpep_pickup_datetime) as trip_date,
    sum(total_amount) as daily_revenue
from taxi_trips
group by trip_date
order by daily_revenue desc
limit 10;

-- average fare by weekday
select
    pickup_day_of_week,
    avg(fare_amount) as avg_fare
from taxi_trips
group by pickup_day_of_week
order by pickup_day_of_week;

-- monthly revenue with growth (window function)
with monthly_revenue as (
    select
        pickup_month,
        sum(total_amount) as revenue
    from taxi_trips
    group by pickup_month
)
select
    pickup_month,
    revenue,
    revenue - lag(revenue) over (order by pickup_month) as revenue_change,
    round(
        (revenue - lag(revenue) over (order by pickup_month)) * 100.0 /
        lag(revenue) over (order by pickup_month),
        2
    ) as growth_percentage
from monthly_revenue
order by pickup_month;
