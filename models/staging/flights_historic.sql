SELECT
    FlightDate,
    Origin,
    Dest,
    SAFE_CAST(DepDelay AS FLOAT64) AS dep_delay,
    SAFE_CAST(ArrDelay AS FLOAT64) AS arr_delay,
    SAFE_CAST(Distance AS FLOAT64) AS distance
FROM `data-management-2-457212.opensky_data.flights_historic_2024`
WHERE FlightDate IS NOT NULL AND Origin IS NOT NULL AND Dest IS NOT NULL
