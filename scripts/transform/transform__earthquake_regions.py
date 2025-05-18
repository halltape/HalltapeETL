from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, trim, lower, md5, to_date, coalesce
import argparse
import logging

parser = argparse.ArgumentParser()
parser.add_argument('--jdbc-url', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-password', required=True)
parser.add_argument('--table-name', required=True)
parser.add_argument('--s3-path-regions', required=True)
parser.add_argument('--s3-path-earthquake', required=True)

logger = logging.getLogger("airflow.task")
args = parser.parse_args()

# Инициализация Spark
spark = SparkSession.builder \
    .appName("S3ToChEarthquakeRegions") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Считаем max(load_date) из ClickHouse
try:
    # Получение максимальной load_date из ClickHouse
    ch_max_df = spark.read \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("dbtable", f"(SELECT MAX(load_date) as max_date FROM {args.table_name})") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .load()

    max_date = ch_max_df.collect()[0]["max_date"].strftime("%Y-%m-%d")
    predicate = "1=1"

    if max_date != '1970-01-01' and max_date:
        predicate = f"load_date > '{max_date}'"

    logger.info(f"🔁 Загружаем данные после {max_date}")

    # Читаем данные
    df = spark.read.json(args.s3_path_earthquake).where(predicate)
    regions = spark.read.parquet(args.s3_path_regions)

    # Преобразование и обогащение
    flattened = df.selectExpr("explode(features) as feature") \
        .select(
            col("feature.id").alias("id"),
            from_unixtime(col("feature.properties.time") / 1000).alias("ts"),
            col("ts").cast("date").alias("load_date"),
            trim(split(col("feature.properties.place"), ",").getItem(0)).alias("place"),
            trim(split(col("feature.properties.place"), ",").getItem(1)).alias("initial_region"),
            col("feature.properties.mag").alias("magnitude"),
            col("feature.properties.felt").alias("felt"),
            col("feature.properties.tsunami").alias("tsunami"),
            col("feature.properties.url").alias("url"),
            col("feature.geometry.coordinates")[0].alias("longitude"),
            col("feature.geometry.coordinates")[1].alias("latitude"),
            col("feature.geometry.coordinates")[2].alias("depth"),
            md5(lower(trim(col("feature.properties.place")))).alias("place_hash")
        )


    # Джоин с регионами
    enriched = flattened.alias("f") \
                        .join(regions.alias("r"), on="place_hash", how="left") \
                        .select(
                            "id",
                            "ts",
                            "place",
                            coalesce(col("r.region"), col("initial_region")).alias("region"),
                            "magnitude",
                            "felt",
                            "tsunami",
                            "url",
                            "longitude",
                            "latitude",
                            "depth",
                            "load_date"
                        )

    # Запись в ClickHouse
    enriched.write \
        .format("jdbc") \
        .option("url", args.jdbc_url) \
        .option("user", args.db_user) \
        .option("password", args.db_password) \
        .option("dbtable", args.table_name) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    print("✅ Данные успешно загружены в ClickHouse.")

except Exception as e:
    logger.error(f"Ошибка: {e}")
