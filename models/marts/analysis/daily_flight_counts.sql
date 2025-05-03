SELECT
  DATE(flight_time) AS flight_day,
  COUNT(*) AS daily_flight_count
FROM {{ ref('flight_combined') }}
WHERE flight_time IS NOT NULL
GROUP BY flight_day
ORDER BY flight_day
