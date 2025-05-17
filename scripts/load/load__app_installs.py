from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--jdbc-url', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-password', required=True)
parser.add_argument('--table-name', required=True)
parser.add_argument('--s3-path', required=True)

args = parser.parse_args()

# Инициализация Spark
spark = SparkSession.builder \
    .appName("JdbcToS3AppInstalls") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Путь к данным
s3_data_path = args.s3_path
s3_checkpoint_path = os.path.join(s3_data_path, "_checkpoint")

# Проверка, есть ли уже файлы в S3
try:
    existing_data_df = spark.read.parquet(s3_data_path)
    max_ts = existing_data_df.selectExpr("MAX(ts) as max_ts").collect()[0]["max_ts"]
    print(f"🔁 Инкрементальная загрузка с ts > {max_ts}")
    predicate = f"ts > timestamp '{max_ts}'"
except Exception as e:
    print("🆕 Данных в S3 нет, загрузим всё из базы.")
    predicate = "1=1"

# Чтение из PostgreSQL
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", args.jdbc_url) \
    .option("user", args.db_user) \
    .option("password", args.db_password) \
    .option("dbtable", args.table_name) \
    .option("fetchsize", 1000) \
    .option("driver", "org.postgresql.Driver") \
    .option("pushDownPredicate", "true") \
    .load() \
    .filter(predicate)

# Обогащение датой для партиционирования
df_with_partition = jdbc_df \
    .withColumn("event_date", to_date(col("ts")))

# Запись в S3 с партиционированием
df_with_partition.write \
    .mode("append") \
    .partitionBy("event_date") \
    .parquet(s3_data_path)

print("✅ Загрузка завершена.")
