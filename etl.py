from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.decorators import task

# Default arguments for the DAG
default_args = {
    'owner': 'nik',
    'start_date': days_ago(1),
    'retries': 1,
}

# SQL queries to create tables and copy data into Snowflake
create_tables_sql = """
CREATE TABLE IF NOT EXISTS stock.raw_data.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);

CREATE TABLE IF NOT EXISTS stock.raw_data.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);
"""

create_stage_sql = """
CREATE OR REPLACE STAGE stock.raw_data.blob_stage
url = 's3://s3-geospatial/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

load_data_sql = """
COPY INTO stock.raw_data.user_session_channel
FROM @stock.raw_data.blob_stage/user_session_channel.csv;

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

    @task
    def create_tables():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(create_tables_sql)

    @task
    def create_stage():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(create_stage_sql)

    @task
    def load_data():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(load_data_sql)

    # Define task dependencies
    create_tables() >> create_stage() >> load_data()