SELECT
    icao24,
    UPPER(TRIM(callsign)) AS callsign,
    origin_country,
    longitude,
    latitude,
    velocity,
    timestamp
FROM `data-management-2-457212.opensky_data.flight_states_staging`
WHERE icao24 IS NOT NULL AND longitude IS NOT NULL AND latitude IS NOT NULL
