import json
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def fetch_weather_and_upload_s3hook():
    # Получаем данные из API Open-Meteo
    """
        API Docs: https://open-meteo.com/en/docs
    """
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 55.75,
            "longitude": 37.62,
            "current_weather": True
        }
    )
    response.raise_for_status()
    data = response.json()

    # Генерируем имя файла
    timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    filename = f"api/weather/moscow_{timestamp}.json"

    # Используем S3Hook
    hook = S3Hook(aws_conn_id='minios3_conn')
    hook.load_string(
        string_data=json.dumps(data),
        key=filename,
        bucket_name='prod',
        replace=True
    )

    print(f"Uploaded to s3://prod/api/{filename}")


default_args = {
    'owner': 'loader',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='Load_API__weather',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    description='API weather to S3',
    tags=['weather', 's3', 'hook']
)

upload_task = PythonOperator(
    task_id='fetch_weather_and_upload',
    python_callable=fetch_weather_and_upload_s3hook,
    dag=dag
)

upload_task