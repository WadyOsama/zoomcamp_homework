# Module 4 Homework Answers: Analytics Engineering

## Question 1: Understanding dbt model resolution

<span style="font-size: 18px;">Answer: **`select * from myproject.raw_nyc_tripdata.ext_green_taxi`**</span>

## Question 2: dbt Variables & Dynamic Models

<span style="font-size: 18px;">Answer: **`Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`**</span>

## Question 3: dbt Data Lineage and Execution

This option does NOT apply because it won't run `dim_zone_lookup` becasue it's not dependant on the staging 

<span style="font-size: 18px;">Answer: **`dbt run --select models/staging/+`**</span>

## Question 4: dbt Macros and Jinja

<span style="font-size: 18px;">Answer: **`ALL of the statements are True except "Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile"`**</span>

## Question 5: Taxi Quarterly Revenue Growth

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select 
    service_type,
    total_amount,
    extract(year from pickup_datetime) AS year,
    extract(quarter from pickup_datetime) AS quarter
  from {{ ref('fact_trips') }}

),
quarterly_revenue  AS (
    select 
        year,
        quarter,
        service_type,
        SUM(total_amount) AS revenue
    from tripdata
    WHERE year IN (2019,2020)
    group by year, quarter, service_type
),
last_revenue AS (
    SELECT *,
    LAG(revenue) over(partition by service_type, quarter ORDER BY year) as pervious_revenue,
    from quarterly_revenue
),
yoy_revenue AS (
    SELECT
        service_type,
        year,
        quarter,
        ((revenue / pervious_revenue) - 1) * 100 AS yoy_revenue
    from last_revenue
    where pervious_revenue is not null
)

select *
from yoy_revenue as y1
where yoy_revenue = (
        select MAX(yoy_revenue)
        from yoy_revenue as y2
        where y1.year = y2.year 
            and y1.service_type = y2.service_type)
    OR yoy_revenue = (
        select MIN(yoy_revenue)
        from yoy_revenue as y2
        where y1.year = y2.year 
            and y1.service_type = y2.service_type)
```

<span style="font-size: 18px;">Answer: **`green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}`**</span>

## Question 6: P97/P95/P90 Taxi Monthly Fare

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select service_type,
    fare_amount,
    extract(year from pickup_datetime) AS year,
    extract(month from pickup_datetime) AS month
  from {{ ref('fact_trips') }}
  where fare_amount > 0
    AND trip_distance > 0
    AND lower(payment_type_description) in ('cash', 'credit card')
),
percntile_fare AS (
    select *,
        PERCENTILE_CONT(fare_amount, 0.97) OVER(partition by service_type, year, month) AS fare_97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER(partition by service_type, year, month) AS fare_95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER(partition by service_type, year, month) AS fare_90
    from tripdata
)

select distinct service_type, fare_97, fare_95, fare_90
from percntile_fare
where year = 2020 and month = 4
```

<span style="font-size: 18px;">Answer: **`green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}`**</span>

## Question 7: Top #Nth longest P90 travel time Location for FHV

<span style="font-size: 18px;">`stg_fhv_tripdata.sql`</span>

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_taxi_data_external') }}
  where dispatching_base_num is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,


    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_number,

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
<span style="font-size: 18px;">`dim_fhv_trips.sql`</span>

```sql
{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.service_type,
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    EXTRACT(year from fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(quarter from fhv_tripdata.pickup_datetime) AS quarter,
    EXTRACT(month from fhv_tripdata.pickup_datetime) AS month,
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
```

<span style="font-size: 18px;">`fct_fhv_monthly_zone_traveltime_p90.sql`</span>

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
    select *,
        TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) AS trip_duration
        from {{ ref('dim_fhv_trips') }}
),
percntile_time AS (
    select *,
        PERCENTILE_CONT(trip_duration, 0.90) OVER(partition by year, month, pickup_locationid, dropoff_locationid) AS time_90
    from tripdata
),
percntile_rank AS (
    select *,
        DENSE_RANK() OVER(partition by year, month, pickup_locationid order by time_90 DESC) AS ranking
    from percntile_time
)

select distinct pickup_zone, dropoff_zone, time_90
from percntile_rank
where pickup_zone in ('Newark Airport', 'SoHo','Yorkville East') AND year = 2019 AND month = 11 AND ranking = 2
```

<span style="font-size: 18px;">Answer: **`LaGuardia Airport, Chinatown, Garment District`**</span>

