-- models/marts/analysis/delay_summary.sql

with base as (
  select * from {{ ref('flight_full_model') }}
)

select
  Origin,
  Dest,
  count(*) as total_flights,
  round(avg(dep_delay), 2) as avg_dep_delay,
  round(avg(arr_delay), 2) as avg_arr_delay
from base
where dep_delay is not null or arr_delay is not null
group by Origin, Dest
order by avg_arr_delay desc
