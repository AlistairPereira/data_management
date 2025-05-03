SELECT
  Origin,
  Dest,
  AVG(velocity) AS avg_velocity
FROM {{ ref('flight_combined') }}
WHERE velocity IS NOT NULL
GROUP BY Origin, Dest
