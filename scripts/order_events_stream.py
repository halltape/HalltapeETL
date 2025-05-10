from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_date
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--kafka-topic', required=True)
parser.add_argument('--kafka-bootstrap', required=True)
parser.add_argument('--s3-path', required=True)

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("KafkaToS3OrderEvents") \
    .getOrCreate()

# Чтение из Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.kafka_bootstrap) \
    .option("subscribe", args.kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Обработка значений (Kafka → JSON → нужные поля)
from pyspark.sql.functions import from_json, schema_of_json

sample_json = '''{
  "payload": {
    "ts": 1715261685,
    "order_id": 123,
    "status": "PAID"
  }
}'''

schema = schema_of_json(sample_json)
print(f'schema = {schema}')

json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")) \
    .select("data.payload.*")

print(f'json_df = {json_df}')

# Преобразуем ts (Unix → UTC Timestamp → Date)
processed_df = json_df \
    .withColumn("ts_utc", from_unixtime(col("ts")).cast("timestamp")) \
    .withColumn("event_date", to_date(col("ts_utc")))  # Для партиций

print(f'processed_df = {processed_df}')

# Запись в S3 с партиционированием
query = processed_df.writeStream \
    .format("parquet") \
    .option("path", args.s3_path) \
    .option("checkpointLocation", args.s3_path + "/_checkpoint/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start()

query.awaitTermination()
