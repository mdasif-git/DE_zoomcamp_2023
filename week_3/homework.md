
--Q1:
SELECT COUNT(1) FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_external` LIMIT 100;

--Q2:
CREATE OR REPLACE TABLE `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat` AS
SELECT * FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_external`;

SELECT COUNT(*) FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat`;

--Q3:
SELECT COUNT(1) FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_external`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

--Q4:
CREATE OR REPLACE TABLE `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat_partitioned` 
PARTITION BY DATE(pickup_datetime) AS
SELECT * FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat`;



--Q5:
SELECT DISTINCT affiliated_base_number 
FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat_partitioned`
WHERE pickup_datetime 
BETWEEN '2019-03-01' and '2019-03-31'

SELECT DISTINCT affiliated_base_number 
FROM `de-zoomcamp-2023-375903.dtc_bq.fhv_taxi_mat`
WHERE pickup_datetime 
BETWEEN '2019-03-01' and '2019-03-31'