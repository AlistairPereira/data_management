SELECT
  Origin,
  Dest,
  COUNT(*) AS total_flights
FROM {{ ref('flight_combined') }}
GROUP BY Origin, Dest
ORDER BY total_flights DESC
LIMIT 10
