SELECT COUNT(1) FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_external` LIMIT 1000

SELECT * FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_external` LIMIT 10

--create non-partitioned native table from external table

CREATE OR REPLACE TABLE `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_non_partitioned` AS
SELECT * FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_external`

--create partitioned native table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_partitioned` 
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT * FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_external`

--Differences in querying the partitoned tables and non-partitioned tables
--scans 60 MB data
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-31'


--scans 0 mb data
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-31'

--take a look inside partitioned tables
SELECT table_name,partition_id,total_rows
FROM `dtc_bq.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name='yellow_trip_native_partitioned'
ORDER BY total_rows DESC


--create clustered partitioned table from external ( was unable to cluster on float type column VendorID)
CREATE OR REPLACE TABLE `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID
AS
SELECT * REPLACE (CAST(VendorID AS INTEGER) AS VendorID) FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_external`;

--query non-partitioned,non-clustered table ( 346 MB)

SELECT COUNT(*)
FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND VendorID = 1


--query partitioned and clustered table ( 263 MB)

SELECT COUNT(*)
FROM `de-zoomcamp-2023-375903.dtc_bq.yellow_trip_native_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND VendorID = 1

