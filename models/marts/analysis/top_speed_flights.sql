-- models/marts/analysis/top_speed_flights.sql

with base as (
  select * from {{ ref('flight_full_model') }}
)

select
  icao24,
  callsign,
  origin_country,
  max(velocity) as top_speed
from base
where velocity is not null
group by icao24, callsign, origin_country
order by top_speed desc
limit 10
