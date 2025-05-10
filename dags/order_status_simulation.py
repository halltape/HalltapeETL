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
    dag_id="order_status_simulation_dag",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция статусов заказов",
    catchup=False,
    tags=['technical', 'order', 'status']
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
        login="airflow",
        password="airflow",
        schema="backend",
        port=5432
    )
    session.add(new_conn)
    session.commit()


def generate_order_events(**kwargs):
    statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
    events = []
    for _ in range(4):
        event = {
            "order_id": random.randint(1000, 9999),
            "status": random.choice(statuses),
            "ts": datetime.now().isoformat()
        }
        events.append(event)
    kwargs['ti'].xcom_push(key='events', value=events)


def insert_order_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="backend_db")
    events = kwargs['ti'].xcom_pull(key='events', task_ids='generate_order_events')
    for e in events:
        hook.run(f"""
            INSERT INTO public.order_events (order_id, status, ts)
            VALUES ({e["order_id"]}, '{e["status"]}', '{e["ts"]}');
        """)


def update_order_events_func(**kwargs):
    hook = PostgresHook(postgres_conn_id="backend_db")

    # Удалим одну строку (последнюю по времени)
    hook.run("""
        DELETE FROM public.order_events
        WHERE id = (
            SELECT id FROM public.order_events
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим одну строку (самую старую)
    hook.run("""
        UPDATE public.order_events
        SET status = 'cancelled', ts = CURRENT_TIMESTAMP
        WHERE id = (
            SELECT id FROM public.order_events
            WHERE status != 'cancelled'
            ORDER BY ts ASC
            LIMIT 1
        );
    """)


create_connection = PythonOperator(
    task_id="create_debezium_db_connection",
    python_callable=create_connection_func,
    dag=dag
)

generate_order_events = PythonOperator(
    task_id="generate_order_events",
    python_callable=generate_order_events,
    dag=dag,
)

insert_order_events = PythonOperator(
    task_id="insert_order_events",
    python_callable=insert_order_events_func,
    provide_context=True,
    dag=dag,
)

modify_order_events = PythonOperator(
    task_id="modify_order_events",
    python_callable=update_order_events_func,
    dag=dag,
)


create_connection >> generate_order_events >> insert_order_events >> modify_order_events