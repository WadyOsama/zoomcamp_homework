# Module 1 Homework Answers: Docker & SQL

## Question 1. Understanding docker first run 

```bash
docker run -it --entrypoint=/bin/bash python:3.12.8
```
```
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

<span style="font-size: 18px;">Answer: **24.3.1**</span>

## Question 2. Understanding Docker networking and docker-compose

Given the following [docker-compose.yaml](docker-compose.yaml) , what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

<span style="font-size: 18px;">Answer: We can use either **postgres:5432** or **db:5432**</span>

##  Prepare Postgres

```bash
docker compose up -d 
```

```bash
docker build -t ingest_data:homework .
```

```bash
docker run --network=homework-network ingest_data:homework postgres postgres postgres 5432 ny_taxi green_trips "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
```

```bash
docker run --network=homework-network ingest_data:homework postgres postgres postgres 5432 ny_taxi zones "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
```

## Question 3. Trip Segmentation Count

```sql
SELECT
	CASE WHEN trip_distance <= 1 THEN '1'
		WHEN trip_distance > 1 AND  trip_distance <= 3 THEN '2'
		WHEN trip_distance > 3 AND  trip_distance <= 7 THEN '3'
		WHEN trip_distance > 7 AND  trip_distance <= 10 THEN '4'
		ELSE '5'
	END AS distance_category,
	count(*) AS number_of_trips
FROM public.green_trips
WHERE date(lpep_pickup_datetime) >= '2019-10-01' AND date(lpep_dropoff_datetime) < '2019-11-01'
GROUP BY distance_category
```

<span style="font-size: 18px;">Answer: **104,802;  198,924;  109,603;  27,678;  35,189**</span>

## Question 4. Longest trip for each day

```sql
SELECT 
	DATE(lpep_pickup_datetime) AS date,
	MAX(trip_distance) AS max_distance
FROM public.green_trips
GROUP BY date
ORDER BY max_distance DESC
LIMIT 1
```
<span style="font-size: 18px;">Answer: **2019-10-31**</span>

## Question 5. Three biggest pickup zones

```sql
SELECT 
	z."Zone",
	CAST(sum(total_amount) AS INT) AS total_amount
FROM green_trips AS g
LEFT JOIN zones AS z
ON g."PULocationID" = z."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Zone"
ORDER BY total_amount DESC
LIMIT 3
```
<span style="font-size: 18px;">Answer: **East Harlem North, East Harlem South, Morningside Heights**</span>

## Question 6. Largest tip

```sql
SELECT
	z_do."Zone",
	MAX(tip_amount) AS largest_tip
FROM green_trips AS g
LEFT JOIN zones AS z_pu
ON g."PULocationID" = z_pu."LocationID"
LEFT JOIN zones AS z_do
ON g."DOLocationID" = z_do."LocationID"
WHERE z_pu."Zone" = 'East Harlem North' AND date(lpep_pickup_datetime) >= '2019-10-01' AND date(lpep_pickup_datetime) < '2019-11-01'
GROUP BY z_do."Zone"
ORDER BY largest_tip DESC
LIMIT 1
```
<span style="font-size: 18px;">Answer: **JFK Airport**</span>


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

<span style="font-size: 18px;">Answer: **terraform init, terraform apply -auto-approve, terraform destroy**</span>
