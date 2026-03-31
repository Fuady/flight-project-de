{{
  config(
    materialized = 'table',
    schema       = 'flights_mart',
  )
}}

/*
  rpt_delay_trend
  ──────────────────────────────────────────────────────────────────────────
  Dashboard Tile 2: Monthly delay trend over time (temporal distribution).
  Powers the line chart showing how delays evolve month-over-month.
*/

with base as (
    select * from {{ ref('fct_flights') }}
),

monthly as (
    select
        year_month,
        flight_year,
        flight_month,
        -- Use first day of month for date axis in charts
        date(flight_year, flight_month, 1)              as month_start_date,

        count(*)                                         as total_flights,
        countif(is_delayed)                              as delayed_flights,
        round(countif(is_delayed) / count(*) * 100, 2)  as delay_pct,

        round(avg(arr_delay), 2)                         as avg_arr_delay_min,
        round(avg(dep_delay), 2)                         as avg_dep_delay_min,

        -- Delay causes breakdown (% contribution)
        round(avg(coalesce(carrier_delay,       0)), 2)  as avg_carrier_delay,
        round(avg(coalesce(weather_delay,       0)), 2)  as avg_weather_delay,
        round(avg(coalesce(nas_delay,           0)), 2)  as avg_nas_delay,
        round(avg(coalesce(late_aircraft_delay, 0)), 2)  as avg_late_aircraft_delay,

        -- 7-day rolling equivalent (monthly grain, 3-month window)
        round(
            avg(avg(arr_delay)) over (
                order by flight_year, flight_month
                rows between 2 preceding and current row
            ), 2
        ) as avg_delay_3mo_rolling

    from base
    group by 1, 2, 3
)

select *
from monthly
order by month_start_date
