-- models/marts/combined/flight_full_model.sql

with states as (
  select * from {{ ref('stg_flight_states') }}
),

data as (
  select * from {{ ref('stg_flight_data') }}
),

historic as (
  select * from {{ ref('stg_flights_historic') }}
)

select
  s.icao24,
  s.callsign,
  s.origin_country,
  s.latitude,
  s.longitude,
  s.velocity,
  s.geo_altitude,
  s.baro_altitude,
  s.ingestion_time,

  d.estArrivalAirport,
  d.estDepartureAirport,
  d.est_arrival_horiz_dist,
  d.est_arrival_vert_dist,
  d.est_departure_horiz_dist,
  d.est_departure_vert_dist,
  d.first_seen,
  d.last_seen,

  h.FlightDate,
  h.Marketing_Airline_Network,
  h.marketing_flight_number,
  h.operating_flight_number,
  h.Origin,
  h.Dest,
  h.dep_delay,
  h.arr_delay,
  h.air_time,
  h.distance
from states s
left join data d
  on s.icao24 = d.icao24 and s.ingestion_time = d.ingestion_time

left join historic h
  on s.callsign = h.Tail_Number
