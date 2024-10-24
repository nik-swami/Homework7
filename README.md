# Airflow Snowflake ETL DAG - README

## Overview

This project uses Airflow to build an ETL pipeline with Snowflake. It consists of two DAGs:

1. **`etl_dag`**: Creates raw tables and loads data from S3 into Snowflake.
2. **`ctas_session_summary`**: Joins raw tables, removes duplicates, and creates a summary table.

## Files

### 1. `etl.py`
- **DAG ID**: `etl_dag`
- **Tasks**:
  - Create `user_session_channel` and `session_timestamp` tables in `stock.raw_data`.
  - Create Snowflake stage from S3.
  - Copy data from S3 into the Snowflake tables.

### 2. `summary.py`
- **DAG ID**: `ctas_session_summary`
- **Task**:
  - Join `user_session_channel` and `session_timestamp`, deduplicate records using `ROW_NUMBER()`, and create the `session_summary` table in `stock.analytics`.

## Setup

1. **Import the following libraries from Python Package Index (PyPI):
   - `apache-airflow-providers-snowflake`
   - `requests`
2. **Snowflake Connection**: Create a Snowflake connection (`snowflake_conn`) in Airflow.
3. **S3 Access**: Ensure S3 bucket (`s3://s3-geospatial/`) has proper permissions.

## Running the DAGs

1. **Run `etl_dag`**: First, to load raw data into Snowflake.
2. **Run `ctas_session_summary`**: After data is loaded, to create the deduplicated `session_summary`.

## Key SQL Features

- **ETL**: Creates raw tables, Snowflake stage, and copies data from S3.
- **Summary**: Joins raw tables, removes duplicates, and creates `session_summary`.

## Notes

- Update S3 bucket paths and Snowflake schema (`stock.raw_data`, `stock.analytics`) as needed.
- Ensure Snowflake connection is properly configured in Airflow.
