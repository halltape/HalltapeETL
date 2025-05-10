from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="kafka_to_s3_streaming",
    default_args=default_args,
    schedule_interval='@once',
    description="Spark Streamig Submit",
    catchup=False,
    tags=['spark', 'streaming']
)


stream_kafka_to_s3 = SparkSubmitOperator(
    task_id='spark_stream_kafka_to_s3',
    application='/opt/airflow/scripts/order_events_stream.py',  # путь до spark-скрипта
    conn_id='spark_default',
    application_args=[
        '--kafka-topic', 'orders.backend.public.order_events',
        '--kafka-bootstrap', 'kafka:29092',
        '--s3-path', 's3a://datalake/order_events/'
    ],
    conf={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g"
    },
    dag=dag
)

stream_kafka_to_s3