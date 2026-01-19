from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="run_metadata_scan",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",  # every 6 hours
    catchup=False,
) as dag:

    run_scan = BashOperator(
        task_id="run_node_scan",
        bash_command="cd /opt/airflow/scanner && node scan.js",
    )
