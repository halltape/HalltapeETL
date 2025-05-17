from pyspark.sql import SparkSession
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
    .appName("JdbcToS3Regions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Путь к данным
s3_data_path = args.s3_path
s3_checkpoint_path = os.path.join(s3_data_path, "_checkpoint")

# Чтение из PostgreSQL
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", args.jdbc_url) \
    .option("user", args.db_user) \
    .option("password", args.db_password) \
    .option("dbtable", args.table_name) \
    .option("fetchsize", 1000) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Запись в S3 с партиционированием
jdbc_df.write \
    .mode("overwrite") \
    .parquet(s3_data_path)

print("✅ Загрузка завершена.")
