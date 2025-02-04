# Module 3 Homework Answers: Data Warehouse and BigQuery

**Load Flow:** I wrote Kestra flow file that you can find [here](./gcp_taxi_parquet.yaml) to upload the files automatically to GCS bucket

<b>BIG QUERY SETUP:</b></br>

```sql
-- Create an external table using the Yellow Taxi Trip Record
CREATE OR REPLACE EXTERNAL TABLE `<PROJECT_ID>.BigQuery_HW_Dataset.external_yellow_taxi`
OPTIONS (
  format = "PARQUET",
    uris = ['gs://<BUCKET_NAME>/yellow_tripdata_*.parquet']
);
```

```sql
-- Create a (regular/materialized) table using the Yellow Taxi Trip Records
CREATE OR REPLACE TABLE `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi` AS
SELECT * FROM `<PROJECT_ID>.BigQuery_HW_Dataset.external_yellow_taxi`;
```

## Question 1:
What is count of records for the 2024 Yellow Taxi Data?

```sql
-- Count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*)
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`;
```

<span style="font-size: 18px;">Answer: **`20,332,093`**</span>

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

```sql 
-- Count the distinct number of PULocationIDs for the entire dataset on both the tables

-- External table
-- This query will process 0 B when run
SELECT COUNT(DISTINCT PULocationID)
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.external_yellow_taxi`;

-- Regular Table
-- This query will process 155.12 MB when run
SELECT COUNT(DISTINCT PULocationID)
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`;
```

<span style="font-size: 18px;">Answer: **`0 MB for the External Table and 155.12 MB for the Materialized Table`**</span>

## Question 3:
Write a query to retrieve the PULocationID form the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

```sql
-- Retrieve the PULocationID from the table
-- This query will process 155.12 MB when run
SELECT PULocationID
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`;

-- Retrieve the PULocationID and DOLocationID on the same table
-- This query will process 310.24 MB when run
SELECT PULocationID, DOLocationID
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`;
```

<span style="font-size: 18px;">Answer: **`BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`**</span>

## Question 4:
How many records have a fare_amount of 0?

```sql
-- Count of records have a fare_amount = 0
SELECT COUNT(fare_amount)
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`
WHERE fare_amount = 0;
```

<span style="font-size: 18px;">Answer: **`8,333`**</span>

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_timedate and order the results by VendorID (Create a new table with this strategy)

```sql
-- Creat a table Partition by tpep_dropoff_timedate and Cluster on VendorID
CREATE OR REPLACE TABLE `<PROJECT_ID>.BigQuery_HW_Dataset.optimized_yellow_taxi` 
PARTITION BY 
  DATE(tpep_dropoff_datetime)
CLUSTER BY 
  VendorID AS
SELECT * FROM `<PROJECT_ID>.BigQuery_HW_Dataset.external_yellow_taxi`;
```

<span style="font-size: 18px;">Answer: **`Partition by tpep_dropoff_timedate and Cluster on VendorID`**</span>

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_timedate
03/01/2024 and 03/15/2024 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

```sql
-- Retrieve the distinct VendorIDs between tpep_dropoff_timedate 03/01/2024 and 03/15/2024 (inclusive)

-- Using the non-partitioned table
-- This query will process 310.24 MB when run.
SELECT DISTINCT VendorID
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Using the partitioned table
-- This query will process 26.84 MB when run.
SELECT DISTINCT VendorID
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.optimized_yellow_taxi`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

<span style="font-size: 18px;">Answer: **`310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`**</span>

## Question 7: 
Where is the data stored in the External Table you created?

<span style="font-size: 18px;">Answer: **`GCP Bucket`**</span>

## Question 8:
It is best practice in Big Query to always cluster your data:

<span style="font-size: 18px;">Answer: **`False`**</span>

## Question 8:
Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
-- Count of records for the 2024 Yellow Taxi Data
-- This query will process 0 B when run.
SELECT count(*)
FROM `<PROJECT_ID>.BigQuery_HW_Dataset.yellow_taxi`; 
```
<span style="font-size: 18px;">Answer: **`The estimation is: This query will process 0 B when run. This is because running this query on non-external table doesn't scan the table but instead it gets the count from the table metadata that BigQuery keeps track of it.`**</span>
