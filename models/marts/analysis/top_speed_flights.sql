SELECT
  callsign,
  velocity,
  longitude,
  latitude,
  flight_time
FROM {{ ref('flight_combined') }}
WHERE velocity IS NOT NULL
ORDER BY velocity DESC
LIMIT 10
