from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import random
from datetime import datetime


default_args = {
    'owner': 'creator',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Create_Postgres__order_events",
    default_args=default_args,
    schedule_interval="* * * * *",
    description="Симуляция статусов заказов",
    catchup=False,
    tags=['technical', 'order_events']
)


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

    # Удалим одну строку (крайнюю по времени)
    hook.run("""
        DELETE FROM public.order_events
        WHERE id = (
            SELECT id FROM public.order_events
            ORDER BY ts DESC
            LIMIT 1
        );
    """)

    # Обновим одну строку (самую новую)
    hook.run("""
        UPDATE public.order_events
        SET status =
            CASE
                WHEN status = 'created' THEN 'processing'
                WHEN status = 'processing' THEN 'shipped'
                WHEN status = 'shipped' THEN 'delivered'
                ELSE status
            END,
            ts = CURRENT_TIMESTAMP
        WHERE status IN ('created', 'processing', 'shipped')
        AND id % 5 = 0;
    """)


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


generate_order_events >> insert_order_events >> modify_order_events