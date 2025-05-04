-- models/marts/analysis/fastest_airlines.sql

with base as (
  select * from {{ ref('flight_full_model') }}
)

select
  Marketing_Airline_Network as airline,
  count(*) as total_flights,
  round(avg(velocity), 2) as avg_velocity
from base
where velocity is not null
group by airline
order by avg_velocity desc
limit 10
