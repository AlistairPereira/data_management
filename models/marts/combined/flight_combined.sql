WITH flight_data AS (
    SELECT * FROM {{ ref('flight_data') }}
),
flight_states AS (
    SELECT * FROM {{ ref('flight_states') }}
),
flight_history AS (
    SELECT * FROM {{ ref('flights_historic') }}
)

SELECT
    fd.callsign,
    fd.estdepartureairport,
    fd.estarrivalairport,
    fd.flight_time,

    fs.longitude,
    fs.latitude,
    fs.velocity,
    fs.timestamp AS state_timestamp,

    fh.FlightDate,
    fh.Origin,
    fh.Dest,
    fh.dep_delay,
    fh.arr_delay,
    fh.distance

FROM flight_data fd
LEFT JOIN flight_states fs
    ON fd.callsign = fs.callsign
    AND ABS(TIMESTAMP_DIFF(fs.timestamp, fd.flight_time, SECOND)) < 300
LEFT JOIN flight_history fh
    ON fd.estdepartureairport = fh.Origin
    AND fd.estarrivalairport = fh.Dest
