from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from datetime import datetime
import pandas as pd


NOW = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
SOURCE = '/opt/synthetic_data'
DATA_LAKE = '/opt/data_lake'

CLICK_DB = 'datamart'
TABLE = 'debit_cards'
CLICKHOUSE_DATA_LAKE = '/var/lib/clickhouse/user_files/debit_cards/partition_date=*/part-*.csv'


client = Client(host='clickhouse', port=9000,
                user='admin', password='admin')


execution_date  = '{{ ds }}'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 1),
    'end_date': datetime(2024, 7, 8),
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


def etl(date):
    spark = spark_session()

    print(f"{NOW} LOADING DATE: {date}\n")
    card = spark.read.csv(f"{SOURCE}/card.csv", header=True, sep=";")\
                .where(f''' load_date = "{date}" ''')
    
    status_card = spark.read.csv(f"{SOURCE}/Status_card.csv", header=True, sep=";")\
                        .where(f''' load_date = "{date}"  ''')
    
    transactions = spark.read.csv(f"{SOURCE}/transactions.csv", header=True, sep=";")\
                        .where(f''' load_date = "{date}"  ''')
    
    
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
                    .withColumn('partition_date', F.lit(date).cast('string').alias('partition_date'))\
                    .withColumn('load_date', F.lit(date).cast('string').alias('load_date'))\
                    .drop('amt', 'status')

    datamart.write.mode("append").partitionBy('partition_date').csv(f'{DATA_LAKE}/debit_cards', header=True)

    logging_data_quality(datamart.count(), date)
    spark.stop()
    print(f"{NOW} LOADED!\n\n")


def insert_into():

    client.timeout = 3000
    client.execute(f""" CREATE DATABASE IF NOT EXISTS {CLICK_DB} """)
    print(f"{NOW} Clickhouse: Database {CLICK_DB} is ready\n\n")

    client.execute(f""" CREATE TABLE IF NOT EXISTS {CLICK_DB}.{TABLE} (
                            card_order_dt String,
                            card_num String,
                            cookie String,
                            url String,
                            transaction_level Boolean NOT NULL,
                            status_flag Boolean NOT NULL,
                            load_date Date NOT NULL
                        ) ENGINE = MergeTree()
                        PARTITION BY toYYYYMM(load_date)
                        ORDER BY card_order_dt
                    """)
    print(f"{NOW} Clickhouse: Table {TABLE} is created\n\n")


    client.execute(f"""
                    INSERT INTO {CLICK_DB}.{TABLE} 
                    SELECT *
                    FROM file('{CLICKHOUSE_DATA_LAKE}', 'CSVWithNames',
                        'card_order_dt String,
                        card_num String,
                        cookie String,
                        url String,
                        transaction_level Boolean,
                        status_flag Boolean,
                        load_date String');
                """)

    print(f"{NOW} Clickhouse: Data is inserted\n\n")


etl_to_data_lake = PythonOperator(
    task_id="etl_to_data_lake",
    python_callable=etl,
    op_kwargs={'date': execution_date},
    dag=dag
)

load_to_clickhouse = PythonOperator(
    task_id="load_to_clickhouse",
    python_callable=insert_into,
    dag=dag
)

etl_to_data_lake >> load_to_clickhouse
