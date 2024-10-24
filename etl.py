from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'nik',
    'start_date': days_ago(1),
    'retries': 1,
}

# SQL queries to create tables and copy data into Snowflake
create_user_session_channel_table = """
CREATE TABLE IF NOT EXISTS stock.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);
"""

create_session_timestamp_table = """
CREATE TABLE IF NOT EXISTS stock.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

create_stage_blob = """
CREATE OR REPLACE STAGE stock.raw_data.blob_stage
url = 's3://s3-geospatial/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

copy_user_session_channel_data = """
COPY INTO stock.raw_data.user_session_channel
FROM @stock.raw_data.blob_stage/user_session_channel.csv;
"""

copy_session_timestamp_data = """
COPY INTO stock.raw_data.session_timestamp
FROM @stock.raw_data.blob_stage/session_timestamp.csv;
"""

# Define the DAG
with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    schedule_interval='@once',  # Run manually 
    catchup=False	
) as dag:

    # Dummy start task
    start_task = DummyOperator(task_id='start')

    # Task to create user_session_channel table
    create_user_session_channel = SnowflakeOperator(
        task_id='create_user_session_channel_table',
        snowflake_conn_id='snowflake_conn',
        sql=create_user_session_channel_table
    )

    # Task to create session_timestamp table
    create_session_timestamp = SnowflakeOperator(
        task_id='create_session_timestamp_table',
        snowflake_conn_id='snowflake_conn',
        sql=create_session_timestamp_table
    )

    # Task to create the Snowflake stage
    create_stage = SnowflakeOperator(
        task_id='create_stage_blob',
        snowflake_conn_id='snowflake_conn',
        sql=create_stage_blob
    )

    # Task to copy data into user_session_channel from S3
    copy_user_session_channel = SnowflakeOperator(
        task_id='copy_user_session_channel_data',
        snowflake_conn_id='snowflake_conn',
        sql=copy_user_session_channel_data
    )

    # Task to copy data into session_timestamp from S3
    copy_session_timestamp = SnowflakeOperator(
        task_id='copy_session_timestamp_data',
        snowflake_conn_id='snowflake_conn',
        sql=copy_session_timestamp_data
    )

    # Dummy end task
    end_task = DummyOperator(task_id='end')

    # Task dependencies
    start_task >> [create_user_session_channel, create_session_timestamp]
    [create_user_session_channel, create_session_timestamp] >> create_stage
    create_stage >> [copy_user_session_channel, copy_session_timestamp]
    [copy_user_session_channel, copy_session_timestamp] >> end_task