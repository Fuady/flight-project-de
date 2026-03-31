{{
  config(
    materialized = 'table',
    schema       = 'flights_mart',
  )
}}

/*
  rpt_delay_by_carrier
  ──────────────────────────────────────────────────────────────────────────
  Dashboard Tile 1: Distribution of delays by airline (categorical).
  Powers the bar chart showing avg delay per carrier.
*/

with base as (
    select * from {{ ref('fct_flights') }}
),

agg as (
    select
        carrier_code,
        carrier_name,

        count(*)                                            as total_flights,
        countif(is_delayed)                                 as delayed_flights,
        round(countif(is_delayed) / count(*) * 100, 2)     as delay_pct,
        round(avg(arr_delay), 2)                            as avg_arr_delay_min,
        round(avg(dep_delay), 2)                            as avg_dep_delay_min,
        round(avg(case when is_delayed then arr_delay end), 2)
                                                            as avg_delay_when_delayed,
        round(avg(carrier_delay),      2)                   as avg_carrier_delay,
        round(avg(weather_delay),      2)                   as avg_weather_delay,
        round(avg(nas_delay),          2)                   as avg_nas_delay,
        round(avg(late_aircraft_delay),2)                   as avg_late_aircraft_delay,

        round(avg(distance), 1)                             as avg_distance_miles,
        min(fl_date)                                        as earliest_date,
        max(fl_date)                                        as latest_date

    from base
    group by 1, 2
)

select *
from agg
order by avg_arr_delay_min desc
