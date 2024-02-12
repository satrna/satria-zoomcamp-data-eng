--- Make new table that lpep_pickup_datetime & lpep_dropoff_datetime turn in to TIMESTAMP type
CREATE OR REPLACE TABLE `green_taxi_2022.new_green_taxi`
AS SELECT 
  VendorID,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS lpep_dropoff_datetime,
  store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
 FROM `zoom-414107.green_taxi_2022.green-taxi`;

 --- Question 2:
 -- Query to count distinct PULocationIDs on the External Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs_external
FROM external_table_name;

-- Query to count distinct PULocationIDs on the Materialized Table
SELECT COUNT(DISTINCT PULocationID) AS PULocationID
FROM `zoom-414107.green_taxi_2022.new_green_taxi`;

--- Question 3:
SELECT COUNT(*)
FROM `zoom-414107.green_taxi_2022.new_green_taxi`
WHERE fare_amount = 0;

---Question 4:
CREATE OR REPLACE TABLE `green_taxi_2022.new_green_taxi_partion_cluster`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID 
AS
SELECT * FROM `zoom-414107.green_taxi_2022.new_green_taxi`;

---Question 5:
---non-partitioned
SELECT DISTINCT(PULocationID) FROM  `zoom-414107.green_taxi_2022.new_green_taxi`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

---Partitioned
SELECT DISTINCT(PULocationID) FROM `zoom-414107.green_taxi_2022.new_green_taxi_partion_cluster`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';