SELECT
    SAFE_CAST(arrivalairportcandidatescount AS INT64) AS arrival_candidates,
    UPPER(TRIM(callsign)) AS callsign,
    estarrivalairport,
    estdepartureairport,
    SAFE_CAST(firstseen AS INT64) AS firstseen,
    SAFE_CAST(lastseen AS INT64) AS lastseen,
    TIMESTAMP_SECONDS(time) AS flight_time,
    icao24
FROM `data-management-2-457212.opensky_data.flight_data_staging`
WHERE icao24 IS NOT NULL AND estarrivalairport IS NOT NULL AND estdepartureairport IS NOT NULL
