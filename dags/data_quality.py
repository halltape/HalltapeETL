from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 1),
    'retries': 1
    }

dag = DAG(
    dag_id="monitoring",
    default_args=default_args,
    schedule_interval='0 15 * * *'
)

load_data = BashOperator(
    task_id="load_to_click",
    bash_command='cd /opt/dbt_click && dbt run --target monitoring -m monitoring',
    dag=dag
)

load_data