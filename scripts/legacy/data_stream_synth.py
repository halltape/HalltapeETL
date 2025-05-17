from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import time
import faker
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'postgres_data_generator',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Запуск каждую минуту
    catchup=False
)

fake = faker.Faker()


def create_database(conn):
    """Создает БД если не существует"""
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow',
        port=5432
    )

    conn.autocommit = True
    cursor = conn.cursor()

    # Проверка наличия базы данных
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'analytics_team'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE analytics_team")

    cursor.close()
    conn.autocommit = False


def create_table(conn):
    # Создание нового соединения с базой analytics_team
    conn_analytics = psycopg2.connect(
        host='postgres',
        database='analytics_team',
        user='airflow',
        password='airflow',
        port=5432
    )

    cursor = conn_analytics.cursor()

    # Создание схемы stage
    cursor.execute("CREATE SCHEMA IF NOT EXISTS stage")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stage.transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliateId VARCHAR(255)
        )
        """)

    cursor.close()
    conn_analytics.commit()
    conn_analytics.close()

def generate_transaction():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "userId": user['username'],
        "timestamp":  datetime.now(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'GBP']),
        'city': fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        'affiliateId': fake.uuid4()
    }


def insert_data(conn, num_records):

    # Создание нового соединения с базой analytics_team
    conn_analytics = psycopg2.connect(
        host='postgres',
        database='analytics_team',
        user='airflow',
        password='airflow',
        port=5432
    )

    cur = conn_analytics.cursor()
    for _ in range(num_records):
        transaction = generate_transaction()

        try:
            cur.execute(
                """
                INSERT INTO stage.transactions
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    transaction["transactionId"],
                    transaction["userId"],
                    transaction["timestamp"],
                    transaction["amount"],
                    transaction["currency"],
                    transaction["city"],
                    transaction["country"],
                    transaction["merchantName"],
                    transaction["paymentMethod"],
                    transaction["ipAddress"],
                    transaction["voucherCode"],
                    transaction["affiliateId"]
                )
            )
        except Exception as e:
            print(f"Ошибка вставки: {e}")
            conn_analytics.rollback()
        else:
            conn_analytics.commit()
    cur.close()
    conn_analytics.close()



init_db = PythonOperator(
    task_id='initialize_database',
    python_callable=create_database,
    dag=dag
)

init_table = PythonOperator(
    task_id='initialize_table',
    python_callable=create_table,
    dag=dag
)

generate_data_and_insert = PythonOperator(
    task_id='generate_records',
    python_callable=insert_data,
    op_kwargs={'num_records': 10},
    dag=dag
)

init_db >> init_table >> generate_data_and_insert
