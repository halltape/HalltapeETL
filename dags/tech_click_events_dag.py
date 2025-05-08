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
    dag_id="tech_click_events_dag",
    default_args=default_args,
    schedule_interval="* * * * *",
    description='Симуляция наполнения таблицы на backend',
    catchup=False,
    tags=['click', 'events']
)



@provide_session
def create_connection_func(session: Session = None, **kwargs):
    conn_id = "analytics_db"

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
        schema="analytics",
        port=5432
    )
    session.add(new_conn)
    session.commit()


def generate_click_events(**kwargs):
    events = []
    for _ in range(4):
        event = {
            "user_id": random.randint(1, 100),
            "event_type": random.choice(["click", "scroll", "hover"]),
            "ts": datetime.now().isoformat()
        }
        events.append(event)
    kwargs['ti'].xcom_push(key='events', value=events)


def insert_click_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="analytics_db")
    events = kwargs['ti'].xcom_pull(key='events', task_ids='generate_click_events')
    for e in events:
        hook.run(f"""
            INSERT INTO public.backend_events (user_id, event_type, ts)
            VALUES ({e["user_id"]}, '{e["event_type"]}', '{e["ts"]}');
        """)


def modify_click_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="analytics_db")

    # Удалим одну строку (последнюю по времени)
    hook.run("""
        DELETE FROM public.backend_events
        WHERE id = (
            SELECT id FROM public.backend_events
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим одну строку (самую старую)
    hook.run("""
        UPDATE public.backend_events
        SET event_type = 'updated_click', ts = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id FROM public.backend_events
            ORDER BY ts ASC
            LIMIT 1
        );
    """)


create_connection = PythonOperator(
    task_id="create_debezium_db_connection",
    python_callable=create_connection_func,
    dag=dag
)

generate_click_events = PythonOperator(
    task_id="generate_click_events",
    python_callable=generate_click_events,
    dag=dag,
)

insert_click_events = PythonOperator(
    task_id="insert_click_events",
    python_callable=insert_click_events_func,
    provide_context=True,
    dag=dag,
)

modify_click_events = PythonOperator(
    task_id="modify_click_events",
    python_callable=modify_click_events_func,
    dag=dag,
)


create_connection >> generate_click_events >> insert_click_events >> modify_click_events