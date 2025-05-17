import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'transformer',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Transform_ObjStore__earthquake_regions",
    default_args=default_args,
    schedule_interval="00 10 * * *",
    description="Spark Submit Inc",
    catchup=False,
    tags=['spark', 'transform']
)


s3_to_ch = SparkSubmitOperator(
    task_id='spark_s3_to_ch',
    application='/opt/airflow/scripts/transform/transform__earthquake_regions.py',
    conn_id='spark_default',
    application_args=[
        '--jdbc-url', 'jdbc:clickhouse://clickhouse:8123/default',
        '--db-user', os.getenv('CLICKHOUSE_USER'),
        '--db-password', os.getenv('CLICKHOUSE_PASSWORD'),
        '--table-name', 'enriched_earthquakes',
        '--s3-path-regions', f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/jdbc/regions/',
        '--s3-path-earthquake', f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/api/earthquake/'
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
        "org.apache.hadoop:hadoop-aws:3.3.2,"
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
        "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"
    ),
    dag=dag
)

s3_to_ch
