SELECT
  Origin,
  COUNT(*) AS total_flights,
  AVG(dep_delay) AS avg_departure_delay,
  AVG(arr_delay) AS avg_arrival_delay
FROM {{ ref('flight_combined') }}
GROUP BY Origin
