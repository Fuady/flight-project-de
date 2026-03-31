{{
  config(
    materialized = 'view',
    schema       = 'flights_staging',
  )
}}

/*
  stg_flights
  ──────────────────────────────────────────────────────────────────────────
  Staging layer: light-touch cleaning on top of the raw BigQuery table.
  - Rename / recast columns
  - Remove cancelled flights (only analyse operated flights)
  - Add surrogate key
  - Filter obviously bad data (future dates, extreme delay values)
*/

with source as (
    select * from {{ source('flights_raw', 'raw_flights') }}
),

renamed as (
    select
        -- Keys & identifiers
        {{ dbt_utils.generate_surrogate_key(['fl_date', 'mkt_carrier', 'mkt_carrier_fl_num', 'origin', 'dest']) }}
            as flight_id,
        fl_date,
        mkt_carrier                         as carrier_code,
        mkt_carrier_fl_num                  as flight_number,
        origin                              as origin_airport,
        dest                                as dest_airport,

        -- Delay metrics (minutes)
        dep_delay,
        arr_delay,
        carrier_delay,
        weather_delay,
        nas_delay,
        security_delay,
        late_aircraft_delay,
        actual_elapsed_time,
        distance,

        -- Derived flags from Spark
        total_delay,
        is_delayed,
        delay_bucket,
        primary_delay_cause,

        -- Date parts
        cast(year  as int64) as flight_year,
        cast(month as int64) as flight_month,

        -- Cancellation
        cast(cancelled as float64)          as cancelled,
        cancellation_code

    from source
),

filtered as (
    select *
    from renamed
    where
        -- Only operated (non-cancelled) flights
        (cancelled is null or cancelled = 0)

        -- Sanity check: flight date must be in the past
        and fl_date < current_date()

        -- Remove extreme outliers (>24h delay is data error)
        and (arr_delay is null or abs(arr_delay) <= 1440)
        and (dep_delay is null or abs(dep_delay) <= 1440)
)

select * from filtered
