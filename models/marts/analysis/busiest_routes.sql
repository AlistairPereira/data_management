-- models/marts/analysis/busiest_routes.sql

with base as (
  select * from {{ ref('flight_full_model') }}
)

select
  Origin,
  Dest,
  count(*) as total_flights
from base
group by Origin, Dest
order by total_flights desc
limit 10
