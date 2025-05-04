-- models/staging/stg_flight_data.sql

with raw as (
  select * from `opensky_data.flight_data_staging_new`
)

select
  cast(arrivalAirportCandidatesCount as int64) as arrival_airport_candidates,
  callsign,
  data_type,
  cast(departureAirportCandidatesCount as int64) as departure_airport_candidates,
  estArrivalAirport,
  cast(estArrivalAirportHorizDistance as int64) as est_arrival_horiz_dist,
  cast(estArrivalAirportVertDistance as int64) as est_arrival_vert_dist,
  estDepartureAirport,
  cast(estDepartureAirportHorizDistance as int64) as est_departure_horiz_dist,
  cast(estDepartureAirportVertDistance as int64) as est_departure_vert_dist,
  cast(firstSeen as int64) as first_seen,
  icao24,
  cast(lastSeen as int64) as last_seen,
  cast(time as int64) as ingestion_time
from raw
where icao24 is not null
