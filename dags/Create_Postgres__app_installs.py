import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.utils.session import provide_session
from airflow.settings import Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import random
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Create_Postgres__app_installs",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция установок приложения",
    catchup=False,
    tags=['technical', 'app_installs']
)

@provide_session
def create_connection_func(session: Session = None, **kwargs):
    conn_id = "backend_db"

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()

    new_conn = Connection(
        conn_id=conn_id,
        conn_type="postgres",
        host="postgres",
        login=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema="backend",
        port=5432
    )
    session.add(new_conn)
    session.commit()

def generate_app_installs(**kwargs):
    os_variants = ["iOS", "Android"]
    installs = []
    for _ in range(4):
        install = {
            "user_id": random.randint(10000, 99999),
            "os": random.choice(os_variants),
            "ts": datetime.now().isoformat()
        }
        installs.append(install)
    kwargs['ti'].xcom_push(key='installs', value=installs)

def insert_app_installs_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="backend_db")
    installs = kwargs['ti'].xcom_pull(key='installs', task_ids='generate_app_installs')
    for i in installs:
        hook.run(f"""
            INSERT INTO public.app_installs (user_id, os, ts)
            VALUES ({i["user_id"]}, '{i["os"]}', '{i["ts"]}');
        """)

create_connection = PythonOperator(
    task_id="create_postgres_connection",
    python_callable=create_connection_func,
    dag=dag
)

generate_app_installs = PythonOperator(
    task_id="generate_app_installs",
    python_callable=generate_app_installs,
    dag=dag,
)

insert_app_installs = PythonOperator(
    task_id="insert_app_installs",
    python_callable=insert_app_installs_func,
    provide_context=True,
    dag=dag,
)

create_connection >> generate_app_installs >> insert_app_installs