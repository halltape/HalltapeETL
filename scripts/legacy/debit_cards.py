from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from datetime import datetime
import pandas as pd
import os
import re

NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
SOURCE = '/opt/synthetic_data'
DATA_LAKE = '/opt/data_lake'

def earliest_date():
    # Регулярное выражение для извлечения дат
    pattern = r'cards_(\d{4})-(\d{2})-(\d{2})\.csv'

    # Получение самой ранней даты из файлов
    earliest_date = None

    # Проход по файлам в директории
    for filename in os.listdir(SOURCE):
        match = re.match(pattern, filename)
        if match:
            year, month, day = match.groups()
            date_str = f"{year}-{month}-{day}"
            date = datetime.strptime(date_str, '%Y-%m-%d')

            # Сохраняем самую раннюю дату
            if earliest_date is None or date < earliest_date:
                earliest_date = date

    # Если дата не найдена, используем дату по умолчанию
    if earliest_date is None:
        earliest_date = datetime(2024, 8, 1)  # Например, 1 августа 2024
    return earliest_date


default_args = {
    'owner': 'airflow',
    'start_date': earliest_date(),
    'retries': 1
    }

dag = DAG(
    dag_id="debit_cards",
    default_args=default_args,
    schedule_interval='0 12 * * *'
)


def spark_session():
    print(f"{NOW} Start spark session\n")

    spark = SparkSession \
            .builder \
            .config("spark.master", "local") \
            .config('spark.executor.cores', '8')\
            .config('spark.executor.memory', '2g')\
            .config('spark.driver.memory', '2g')\
            .config('spark.dynamicAllocation.minExecutors', '4')\
            .appName("debit_cards") \
    .getOrCreate()

    print(f"{NOW} Spark is started\n")
    print(f"{NOW} ===> Spark UI: {spark.sparkContext.uiWebUrl} <===\n")
    return spark


def logging_data_quality(total_rows, date):
    logs_dict = {'datamart_name': ['debit_cards'],
                 'load_date': [date],
                 'total_rows': [total_rows],
                 'created_at': [NOW]}
    pd.DataFrame(data=logs_dict).to_csv(f'{DATA_LAKE}/monitoring/logs_datamart.csv',
                                        mode='a', index=False, sep=',', header=False)
    print(f"{NOW} === DATA QUALITY CHECK ===\n")


def etl(execution_date):
    spark = spark_session()

    print(f"{NOW} LOADING DATE: {execution_date}\n")
    card = spark.read.csv(f"{SOURCE}/cards_{execution_date}.csv", header=True, sep=";")\
                .where(f''' load_date = "{execution_date}" ''')
    
    status_card = spark.read.csv(f"{SOURCE}/cards_status_{execution_date}.csv", header=True, sep=";")\
                        .where(f''' load_date = "{execution_date}"  ''')
    
    transactions = spark.read.csv(f"{SOURCE}/transactions_{execution_date}.csv", header=True, sep=";")\
                        .where(f''' load_date = "{execution_date}"  ''')
    
    
    first_trx = transactions.groupBy('card_num')\
                            .agg(F.min('transaction_datetime').alias('transaction_datetime'))
    
    
    first_trx_info = first_trx.join(transactions, ['card_num','transaction_datetime'], 'inner')
    
    dt_trx = first_trx_info.select('card_num', 'amount', '*')
    df_st = status_card.select('card_num', 'card_num_md5', 'status')
    
    result_df = dt_trx\
                    .join(df_st, "card_num", "inner")\
                    .join(card, "card_num_md5", "right")\
                    .drop('card_num_md5')
    
    final_df = result_df\
                    .where(''' card_num IS NOT NULL ''')\
                    .groupBy('card_num',
                             'transaction_datetime',
                             'status',
                             'card_order_dt',
                             'url',
                             'cookie')\
                    .agg(F.max('amount').alias('amt'))
    
    
    datamart = final_df\
                    .select('card_order_dt',
                            'card_num',
                            'cookie',
                            'url',
                            'amt',
                            'status')\
                    .withColumn('transaction_level',
                                    F.when(F.col('amt') > 300, True).otherwise(False))\
                    .withColumn('status_flag',
                                    F.when(F.col('status') == "выдана", True).otherwise(False))\
                    .withColumn('partition_date', F.lit(execution_date).cast('string').alias('partition_date'))\
                    .withColumn('load_date', F.lit(execution_date).cast('string').alias('load_date'))\
                    .drop('amt', 'status')

    datamart.write.mode("append").partitionBy('partition_date').csv(f'{DATA_LAKE}/debit_cards', header=True)

    logging_data_quality(datamart.count(), execution_date)
    spark.stop()
    print(f"{NOW} LOADED!\n\n")


etl_to_data_lake = PythonOperator(
    task_id="etl_to_data_lake",
    python_callable=etl,
    op_kwargs={'execution_date': "{{ ds }}"},
    dag=dag
)

load_to_clickhouse = BashOperator(
    task_id="load_to_clickhouse",
    bash_command='cd /opt/dbt_click && dbt run --target datamart -m datamart --vars "{partition_date: "{{ ds }}"}"',
    dag=dag
)

etl_to_data_lake >> load_to_clickhouse
