-- models/staging/stg_flights_historic.sql

with raw as (
  select * from `data-management-2-457212.opensky_data.flights_historic_2024`
)

select
  cast(`Year` as int64) as year,
  cast(`Month` as int64) as month,
  cast(`DayOfMonth` as int64) as day_of_month,
  cast(`DayOfWeek` as int64) as day_of_week,
  `FlightDate`,
  `Marketing_Airline_Network`,
  cast(`Flight_Number_Marketing_Airline` as int64) as marketing_flight_number,
  `Tail_Number`,
  cast(`Flight_Number_Operating_Airline` as int64) as operating_flight_number,
  `OriginAirportID`,
  `Origin`,
  `OriginCityName`,
  `OriginState`,
  `OriginStateName`,
  `DestAirportID`,
  `Dest`,
  `DestCityName`,
  `DestState`,
  `DestStateName`,
  cast(`DepTime` as int64) as dep_time,
  cast(`DepDelay` as int64) as dep_delay,
  cast(`TaxiOut` as int64) as taxi_out,
  cast(`TaxiIn` as int64) as taxi_in,
  cast(`ArrTime` as int64) as arr_time,
  cast(`ArrDelay` as int64) as arr_delay,
  cast(`CRSElapsedTime` as int64) as crs_elapsed_time,
  cast(`ActualElapsedTime` as int64) as actual_elapsed_time,
  cast(`AirTime` as int64) as air_time,
  cast(`Distance` as int64) as distance
from raw
where `FlightDate` is not null
