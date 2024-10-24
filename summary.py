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

# SQL query to create the session_summary table with a JOIN and duplicate check
create_session_summary_table = """
CREATE TABLE IF NOT EXISTS stock.analytics.session_summary AS
WITH ranked_sessions AS (
    SELECT 
        usc.userId, 
        usc.sessionId, 
        usc.channel, 
        st.ts,
        ROW_NUMBER() OVER (PARTITION BY usc.sessionId ORDER BY st.ts DESC) AS row_num
    FROM stock.raw_data.user_session_channel usc
    JOIN stock.raw_data.session_timestamp st
    ON usc.sessionId = st.sessionId
)
SELECT
    userId,
    sessionId,
    channel,
    ts
FROM ranked_sessions
WHERE row_num = 1;
"""

# Define the DAG
with DAG(
    dag_id='ctas_session_summary',
    default_args=default_args,
    schedule_interval='@once',  # Run manually 
    catchup=False
) as dag:

    # Dummy start task
    start_task = DummyOperator(task_id='start')

    # Task to create session_summary table with JOIN and duplicate check
    create_session_summary = SnowflakeOperator(
        task_id='create_session_summary_table',
        snowflake_conn_id='snowflake_conn',
        sql=create_session_summary_table
    )

    # Dummy end task
    end_task = DummyOperator(task_id='end')

    # Task dependencies
    start_task >> create_session_summary >> end_task