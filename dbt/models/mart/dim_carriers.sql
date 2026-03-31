{{
  config(
    materialized = 'table',
    schema       = 'flights_mart',
  )
}}

/*
  dim_carriers
  ──────────────────────────────────────────────────────────────────────────
  Static dimension: maps IATA carrier codes to full airline names.
  This avoids a BTS lookup table dependency; expand as needed.
*/

with carrier_map as (
    select * from (
        values
            ('AA', 'American Airlines'),
            ('AS', 'Alaska Airlines'),
            ('B6', 'JetBlue Airways'),
            ('DL', 'Delta Air Lines'),
            ('F9', 'Frontier Airlines'),
            ('G4', 'Allegiant Air'),
            ('HA', 'Hawaiian Airlines'),
            ('NK', 'Spirit Airlines'),
            ('OH', 'PSA Airlines'),
            ('OO', 'SkyWest Airlines'),
            ('QX', 'Horizon Air'),
            ('UA', 'United Airlines'),
            ('WN', 'Southwest Airlines'),
            ('YV', 'Mesa Airlines'),
            ('YX', 'Republic Airways'),
            ('9E', 'Endeavor Air'),
            ('MQ', 'Envoy Air')
    ) as t(carrier_code, carrier_name)
)

select
    carrier_code,
    carrier_name
from carrier_map
