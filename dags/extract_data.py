from airflow import DAG
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import json


client = Client(host='clickhouse', port=9000,
                user='admin', password='admin')

dag = DAG(
    dag_id="halltape_etl",
    start_date=datetime(2024,7,20),
    schedule_interval="@daily"
)


def get_data_from_api():
    url = 'https://api.spacexdata.com/v3/launches'
    response = requests.get(url)
    launches_json = response.json()

    with open('/opt/data_lake/launches.json', 'w') as write:
        json.dump(launches_json, write)

    data = {
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    df.to_csv('/opt/data_lake/output.csv', index=False)


def create_schema():
    client.timeout = 3000
    client.execute("""CREATE DATABASE IF NOT EXISTS raw""")


def create_table():
    client.timeout = 3000
    client.execute("""
        CREATE TABLE IF NOT EXISTS raw.my_table (
            id UInt64,
            name String,
            dt Date
            )
        ENGINE = MergeTree
        ORDER BY (id, dt)
        PARTITION BY toYYYYMM(dt)
        """)

def insert_into_table():
    client.timeout = 3000
    client.execute("""
        INSERT INTO raw.my_table (id, name, dt)
        SELECT
            number AS id,
            arrayJoin(['Alice', 'Bob', 'Charlie', 'David', 'Eve']) AS name,
            toDate(toStartOfHour(now()) - number * 60 * 60) AS dt
        FROM numbers(1000)
        """)

stage1 = PythonOperator(
    task_id="get_data_from_api",
    python_callable=get_data_from_api,
    dag=dag
)

stage2 = PythonOperator(
    task_id="create_schema",
    python_callable=create_schema,
    dag=dag
)

stage3 = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag
)

stage4 = PythonOperator(
    task_id="insert_into_table",
    python_callable=insert_into_table,
    dag=dag
)

stage1 >> stage2 >> stage3 >> stage4





# CREATE TABLE raw.debit_card (
#     card_order_dt String,
#     card_num String,
#     cookie String,
#     url String,
#     transaction_level Boolean NOT NULL,
#     status_flag Boolean NOT NULL,
#     load_date Date NOT NULL
# ) ENGINE = MergeTree()
# PARTITION BY toYYYYMM(load_date)
# ORDER BY card_order_dt;



# INSERT INTO raw.debit_card
# SELECT *
# FROM file('/var/lib/clickhouse/user_files/debit_card/partition_date=*/part-*.csv', 'CSVWithNames',
#     'card_order_dt String,
# 	card_num String,
# 	cookie String,
# 	url String,
# 	transaction_level Boolean,
# 	status_flag Boolean,
# 	load_date String');
