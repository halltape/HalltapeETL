import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Load_Kafka__order_events",
    default_args=default_args,
    schedule_interval='@once',
    description="Spark Submit",
    catchup=False,
    tags=['spark', 'streaming']
)


@provide_session
def create_spark_connection(session: Session = None, **kwargs):
    conn_id = "spark_default"

    # Удаляем, если уже есть
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()

    # Создаем новое подключение
    new_conn = Connection(
        conn_id=conn_id,
        conn_type="spark",
        host="local",
        extra='''{
                    "spark-binary": "spark-submit",
                    "deploy-mode": "client"
                }'''
    )

    session.add(new_conn)
    session.commit()



create_spark_connection = PythonOperator(
    task_id="create_spark_connection",
    python_callable=create_spark_connection,
    dag=dag
)

stream_kafka_to_s3 = SparkSubmitOperator(
    task_id='spark_stream_kafka_to_s3',
    application='/opt/airflow/scripts/ETL__order_events.py',  # путь до spark-скрипта
    conn_id='spark_default',
    application_args=[
        '--kafka-topic', 'orders.backend.public.order_events',
        '--kafka-bootstrap', 'kafka:29093',
        '--s3-path', 's3a://datalake/order_events/'
    ],
    conf={
        "spark.executor.instances": "1",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1",
        "spark.driver.memory": "1g",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    },
    packages=(
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
        "org.apache.hadoop:hadoop-aws:3.3.2,"
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ),
    dag=dag
)

create_spark_connection >> stream_kafka_to_s3

# "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
# "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),