import json
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def fetch_and_upload_with_variable(**kwargs):

    # Получаем последнюю дату из переменной
    last_date_str = Variable.get("earthquake_last_loaded_date", default_var="2025-05-16")
    starttime = datetime.strptime(last_date_str, "%Y-%m-%d")
    endtime = starttime + timedelta(days=1)

    if endtime.date() < datetime.now().date():
        print("🟡 Новых данных пока нет.")
        return

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        filename = f"earthquake/events_{starttime.strftime('%Y-%m-%d')}.json"

        hook = S3Hook(aws_conn_id='minios3_conn')
        hook.load_string(
            string_data=json.dumps(data),
            key=filename,
            bucket_name='prod',
            replace=True
        )

        print(f"✅ Данные за {starttime} загружены в s3://prod/{filename}")

        # Обновляем переменную
        Variable.set("earthquake_last_loaded_date", starttime)
        print(f"🔁 Обновлена переменная: earthquake_last_loaded_date = {starttime}")

    except Exception as e:
        print(f"❌ Ошибка запроса или загрузки: {e}")


default_args = {
    'owner': 'earthquake-loader',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='Load_API__earthquake_data_with_variable',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='API earthquake to S3',
    tags=['earthquake', 's3', 'airflow']
)

fetch_and_upload_task = PythonOperator(
    task_id='fetch_and_upload_with_variable',
    python_callable=fetch_and_upload_with_variable,
    dag=dag
)