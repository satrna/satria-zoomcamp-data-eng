--- Question 0 ---
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--- Question 1 ---
CREATE MATERIALIZED VIEW taxi_zone_calculation AS
    with t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) as trip_time,
        pick_zone.zone as taxi_pickup_zone,
        drop_zone.zone as taxi_drop_zone
        FROM trip_data
        INNER JOIN taxi_zone as pick_zone
            ON trip_data.pulocationid = pick_zone.location_id
        INNER JOIN taxi_zone as drop_zone
            ON trip_data.dolocationid = drop_zone.location_id
    )
    SELECT taxi_pickup_zone, taxi_drop_zone, AVG(trip_time) as avg_trip, MAX(trip_time) as max_trip, MIN(trip_time) as min_trip
    FROM t
    GROUP BY taxi_pickup_zone, taxi_drop_zone
    ORDER BY avg_trip DESC;

--- Question 2 ---
CREATE MATERIALIZED VIEW taxi_zone_calculation_with_count AS
    with t AS (
        SELECT (tpep_dropoff_datetime - tpep_pickup_datetime) as trip_time,
        pick_zone.zone as taxi_pickup_zone,
        drop_zone.zone as taxi_drop_zone
        FROM trip_data
        INNER JOIN taxi_zone as pick_zone
            ON trip_data.pulocationid = pick_zone.location_id
        INNER JOIN taxi_zone as drop_zone
            ON trip_data.dolocationid = drop_zone.location_id
    )
    SELECT taxi_pickup_zone, taxi_drop_zone, AVG(trip_time) as avg_trip, MAX(trip_time) as max_trip, MIN(trip_time) as min_trip, count(*) as num_trip
    FROM t
    GROUP BY taxi_pickup_zone, taxi_drop_zone
    ORDER BY avg_trip DESC;

--- Question 3 ---
CREATE MATERIALIZED VIEW latest_pickup_time AS
    SELECT tpep_pickup_datetime as pickup_time
    FROM trip_data
    WHERE tpep_pickup_datetime = (
        SELECT MAX(tpep_pickup_datetime)
        FROM trip_data
    );

CREATE MATERIALIZED VIEW bussiest_zone_17_hours_before AS
    SELECT pick_zone.zone as taxi_pickup_zone, count(*) as num_rides
    FROM trip_data
    INNER JOIN taxi_zone as pick_zone
        ON pick_zone.location_id = trip_data.pulocationid
    WHERE trip_data.tpep_pickup_datetime >= (
        SELECT pickup_time - INTERVAL '17' HOUR
        FROM latest_pickup_time
    )
    GROUP BY taxi_pickup_zone
    ORDER BY num_rides
    LIMIT 3;



