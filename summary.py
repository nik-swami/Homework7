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

    @task
    def create_session_summary_table_task():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(create_session_summary_table)

    # Call the task directly
    create_session_summary_table_task()