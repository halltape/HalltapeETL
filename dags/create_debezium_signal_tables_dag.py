from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.connection import Connection
from airflow.utils.session import provide_session
from airflow.settings import Session
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="create_debezium_signal_tables",
    default_args=default_args,
    schedule_interval=None,
    description='Создание heartbeat и signal таблиц для Debezium',
    catchup=False,
    tags=['debezium', 'init']
)


@provide_session
def create_connection_func(session: Session = None, **kwargs):
    conn_id = "postgres_db"

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()

    new_conn = Connection(
        conn_id=conn_id,
        conn_type="postgres",
        host="postgres",
        login="airflow",
        password="airflow",
        schema="postgres",
        port=5432
    )
    session.add(new_conn)
    session.commit()


create_connection = PythonOperator(
    task_id="create_airflow_db_connection",
    python_callable=create_connection_func,
    dag=dag
)


create_debezium_db = SQLExecuteQueryOperator(
    task_id='create_debezium_db',
    conn_id='postgres_db',
    sql="""
        CREATE DATABASE IF NOT EXISTS debezium;
    """,
    dag=dag
)


create_signal_table = SQLExecuteQueryOperator(
    task_id='create_signal_table',
    conn_id='postgres_db',
    sql="""
        CREATE TABLE IF NOT EXISTS public.dbz_signal (
            id   VARCHAR(64),
            type VARCHAR(32),
            data VARCHAR(2048)
        );
    """,
    dag=dag
)

create_heartbeat_table = SQLExecuteQueryOperator(
    task_id='create_heartbeat_table',
    conn_id='postgres_db',
    sql="""
        CREATE TABLE IF NOT EXISTS public.dbz_heartbeat (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

create_connection >> create_debezium_db >> create_signal_table >> create_heartbeat_table