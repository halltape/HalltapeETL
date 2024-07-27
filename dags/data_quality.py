from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime


NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

CLICK_DB = 'monitoring'
TABLE = 'logs_datamart'
CLICKHOUSE_DATA_LAKE = '/var/lib/clickhouse/user_files/monitoring/logs_datamart.csv'


client = Client(host='clickhouse', port=9000,
                user='admin', password='admin')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 1),
    'retries': 1
    }


dag = DAG(
    dag_id="monitoring",
    default_args=default_args,
    schedule_interval='0 15 * * *'
)


def create_insert_data():
    client.timeout = 3000
    client.execute(f""" CREATE DATABASE IF NOT EXISTS {CLICK_DB} """)
    print(f"{NOW} Clickhouse: Database {CLICK_DB} is ready\n\n")

    client.execute(f""" CREATE TABLE IF NOT EXISTS {CLICK_DB}.{TABLE} (
                            datamart_name String,
                            load_date String,
                            total_rows Int64,
                            created_at String
                        ) ENGINE = MergeTree()
                        PARTITION BY datamart_name
                        ORDER BY created_at
                    """)
    print(f"{NOW} Clickhouse: Table {TABLE} is created\n\n")

    client.execute(f"""
                    INSERT INTO {CLICK_DB}.{TABLE} 
                    SELECT *
                    FROM file('{CLICKHOUSE_DATA_LAKE}', 'CSV',
                        'datamart_name String,
                        load_date String,
                        total_rows Int64,
                        created_at String');
                    """)

    print(f"{NOW} Clickhouse: Data is inserted\n\n")

def delete_rows():
    client.timeout = 3000

    client.execute(f""" CREATE TABLE IF NOT EXISTS {CLICK_DB}.{TABLE}_temp (
                            datamart_name String,
                            load_date String,
                            total_rows Int64,
                            created_at String
                        ) ENGINE = MergeTree()
                        PARTITION BY datamart_name
                        ORDER BY created_at
                    """)

    client.execute(f""" INSERT INTO {CLICK_DB}.{TABLE}_temp (datamart_name, load_date, total_rows, created_at)
                        SELECT DISTINCT
                            datamart_name,
                            load_date, 
                            total_rows,
                            MAX(created_at) AS created_at
                        FROM {CLICK_DB}.{TABLE}
                        GROUP BY datamart_name, load_date, total_rows """)
    
    client.execute(f""" DROP TABLE {CLICK_DB}.{TABLE} """)
    print(f"{NOW} Clickhouse: {CLICK_DB}.{TABLE} is deleted!\n\n")
    client.execute(f""" RENAME TABLE {CLICK_DB}.{TABLE}_temp TO {CLICK_DB}.{TABLE} """)
    print(f"{NOW} Clickhouse: Data is deduplicated\n\n")

create_table = PythonOperator(
    task_id="create_table",
    python_callable=create_insert_data,
    dag=dag
)


deduplicate = PythonOperator(
    task_id="deduplicate",
    python_callable=delete_rows,
    dag=dag
)

create_table >> deduplicate