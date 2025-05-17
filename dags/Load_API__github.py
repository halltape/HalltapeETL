import json
import requests
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def fetch_github_and_upload_s3hook():
    # Получаем данные из API Github Events
    """
        API Docs: https://docs.github.com/en/rest/activity/events?apiVersion=2022-11-28#list-public-events
    """
    url = "https://api.github.com/events"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        events = response.json()

        # Генерируем имя файла
        timestamp = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        filename = f"api/github/events_{timestamp}.json"

        # Используем S3Hook
        hook = S3Hook(aws_conn_id='minios3_conn')
        hook.load_string(
            string_data=json.dumps(events),
            key=filename,
            bucket_name='prod',
            replace=True
        )

        print(f"Uploaded to s3://prod/{filename}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")


default_args = {
    'owner': 'loader',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='Load_API__github',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    description='API github to S3',
    tags=['github', 's3', 'hook']
)

upload_task = PythonOperator(
    task_id='fetch_github_and_upload',
    python_callable=fetch_github_and_upload_s3hook,
    dag=dag
)

upload_task