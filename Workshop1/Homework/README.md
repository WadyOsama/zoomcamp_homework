# Workshop 1 Homework Answers: Data Ingestion


## Install dlt:

```python
!pip install dlt[duckdb]
```

## Question 1: dlt Version

```python
import dlt
print("dlt version:", dlt.__version__)
```

<span style="font-size: 18px;">Answer: **`dlt version: 1.6.1`**</span>

## Question 2: Define & Run the Pipeline (NYC Taxi API)

How many tables were created?

```python 
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi)
print(load_info)

import duckdb
from google.colab import data_table
data_table.enable_dataframe_formatter()

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

conn.sql("DESCRIBE").df()
```

<span style="font-size: 18px;">Answer: **`4 Tables`**</span>

## Question 3: Explore the loaded data

What is the total number of records extracted?

```python
df = pipeline.dataset(dataset_type="default").rides.df()
df
```

<span style="font-size: 18px;">Answer: **`10000 Records`**</span>

## Question 4: Trip Duration Analysis
What is the average trip duration?

```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    print(res)
```

<span style="font-size: 18px;">Answer: **`12.3049`**</span>
