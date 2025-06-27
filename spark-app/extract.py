from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date
import argparse
from datetime import datetime
import sys

spark = SparkSession.builder \
    .appName("SteamReviewByDay") \
    .config("spark.mongodb.read.connection.uri", "mongodb://NamHy:NamHyCute@host.docker.internal:27017/steam_db?authSource=admin&tls=false") \
    .config("spark.sql.catalog.steam_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.steam_catalog.type", "hive") \
    .config("spark.sql.catalog.steam_catalog.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.steam_catalog.warehouse", "hdfs://hadoop-master:9000/user/root/bronze") \
    .getOrCreate()

# Parse extraction day
parser = argparse.ArgumentParser()
parser.add_argument("--day", required=True, help="Extraction Date in format YYYY-MM-DD")
args = parser.parse_args()
extraction_day = args.day

# Validate date format
try:
    extraction_day = datetime.strptime(extraction_day, "%Y-%m-%d")
except ValueError:
    print(f"Invalid date format: '{extraction_day}'. Expected format: YYYY-MM-DD.")
    sys.exit(1)    


# Try reading a sample from MongoDB
try:
    df = spark.read \
    .format("mongodb") \
    .option("database", "steam_db") \
    .option("collection", "steam_review_sample") \
    .load()
except Exception as e:
    print("Connection failed:")
    print(e)
else:
    print("Connection successful.")


df = df.withColumn("created_day", to_date(from_unixtime("timestamp_created")))

df.createOrReplaceTempView("reviews")

extraction_data = spark.sql(
    f'''
    SELECT *
    FROM reviews
    WHERE DATE(created_day) = DATE('{extraction_day}')
    '''
)

print("Size of extraction_data:", extraction_data.count())  # Trigger the action to read data

extraction_data.show(5)

spark.sql("CREATE DATABASE IF NOT EXISTS steam_catalog.bronze")

extraction_data.writeTo("steam_catalog.bronze.steam_reviews") \
  .using("iceberg") \
  .partitionedBy("created_day") \
  .overwritePartitions()  

