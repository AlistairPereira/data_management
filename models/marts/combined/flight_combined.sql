-- models/marts/combined/flight_combined.sql

with flight_data as (
  select * from {{ ref('stg_flight_data') }}
),

flight_states as (
  select * from {{ ref('stg_flight_states') }}
)

select
  fs.icao24,
  fs.callsign,
  fs.origin_country,
  fs.latitude,
  fs.longitude,
  fs.velocity,
  fs.geo_altitude,
  fs.baro_altitude,
  fs.ingestion_time,

  fd.estArrivalAirport,
  fd.estDepartureAirport,
  fd.est_arrival_horiz_dist,
  fd.est_departure_horiz_dist,
  fd.first_seen,
  fd.last_seen
from flight_states fs
left join flight_data fd
  on fs.icao24 = fd.icao24
  and fs.ingestion_time = fd.ingestion_time
