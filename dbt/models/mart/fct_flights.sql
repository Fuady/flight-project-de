{{
  config(
    materialized       = 'table',
    schema             = 'flights_mart',
    partition_by       = {
      "field": "fl_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by         = ["carrier_code", "origin_airport"],
    require_partition_filter = false,
  )
}}

/*
  fct_flights
  ──────────────────────────────────────────────────────────────────────────
  Core fact table. Grain: one row per operated flight leg.

  Partitioned by fl_date (MONTH):
    → Dashboard queries always filter by date range; monthly partitioning
      prunes entire date segments, reducing bytes scanned.

  Clustered by carrier_code + origin_airport:
    → The two most common group-by / filter columns in dashboard queries.
      Clustering sorts within each partition, so BQ skips blocks that
      don't match the filter — further reduces cost.
*/

with stg as (
    select * from {{ ref('stg_flights') }}
),

dim_carriers as (
    select * from {{ ref('dim_carriers') }}
)

select
    -- Surrogate key
    stg.flight_id,

    -- Time dimensions
    stg.fl_date,
    stg.flight_year,
    stg.flight_month,
    format_date('%Y-%m', stg.fl_date) as year_month,

    -- Carrier
    stg.carrier_code,
    coalesce(dc.carrier_name, stg.carrier_code) as carrier_name,

    -- Route
    stg.origin_airport,
    stg.dest_airport,
    concat(stg.origin_airport, ' → ', stg.dest_airport) as route,
    stg.distance,

    -- Delay details
    stg.dep_delay,
    stg.arr_delay,
    stg.carrier_delay,
    stg.weather_delay,
    stg.nas_delay,
    stg.security_delay,
    stg.late_aircraft_delay,
    stg.total_delay,
    stg.is_delayed,
    stg.delay_bucket,
    stg.primary_delay_cause,
    stg.actual_elapsed_time

from stg
left join dim_carriers dc
    on stg.carrier_code = dc.carrier_code
