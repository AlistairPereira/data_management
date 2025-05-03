SELECT
  CASE
    WHEN arr_delay < 5 THEN 'On time'
    WHEN arr_delay BETWEEN 5 AND 30 THEN 'Slightly delayed'
    WHEN arr_delay BETWEEN 30 AND 120 THEN 'Moderately delayed'
    ELSE 'Heavily delayed'
  END AS delay_category,
  COUNT(*) AS count
FROM {{ ref('flight_combined') }}
GROUP BY delay_category
