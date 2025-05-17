import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'loader',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    dag_id="Load_JDBC__app_installs",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    description="Spark Submit Inc",
    catchup=False,
    tags=['spark', 'batch']
)


jdbc_to_s3 = SparkSubmitOperator(
    task_id='spark_jdbc_to_s3',
    application='/opt/airflow/scripts/load/load__app_installs.py',
    conn_id='spark_default',
    application_args=[
        '--jdbc-url', 'jdbc:postgresql://postgres:5432/backend',
        '--db-user', os.getenv('POSTGRES_USER'),
        '--db-password', os.getenv('POSTGRES_PASSWORD'),
        '--table-name', 'public.app_installs',
        '--s3-path', f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/jdbc/app_installs/'
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
        "org.postgresql:postgresql:42.5.0"
    ),
    dag=dag
)

jdbc_to_s3
