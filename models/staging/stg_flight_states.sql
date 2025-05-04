-- models/staging/stg_flight_states.sql

with raw as (
  select * from `opensky_data.flight_states_staging_new`
)

select
  icao24,
  callsign,
  origin_country,
  cast(time_position as int64) as time_position,
  cast(last_contact as int64) as last_contact,
  cast(longitude as float64) as longitude,
  cast(latitude as float64) as latitude,
  cast(baro_altitude as float64) as baro_altitude,
  on_ground,
  cast(velocity as float64) as velocity,
  cast(true_track as float64) as true_track,
  cast(vertical_rate as float64) as vertical_rate,
  sensors,
  cast(geo_altitude as float64) as geo_altitude,
  squawk,
  spi,
  position_source,
  cast(time as int64) as ingestion_time,
  data_type
from raw
where icao24 is not null
