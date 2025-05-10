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
    dag_id="Load_JDBC__app_installs",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    description="Spark Submit",
    catchup=False,
    tags=['spark', 'batch']
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

jdbc_to_s3 = SparkSubmitOperator(
    task_id='spark_jdbc_to_s3',
    application='/opt/airflow/scripts/extract__app_installs.py',
    conn_id='spark_default',
    application_args=[
        '--jdbc-url', 'jdbc:postgresql://postgres:5432/backend',
        '--db-user', os.getenv('POSTGRES_USER'),
        '--db-password', os.getenv('POSTGRES_PASSWORD'),
        '--table-name', 'public.app_installs',
        '--s3-path', f's3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/stage/app_installs/'
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

create_spark_connection >> jdbc_to_s3
