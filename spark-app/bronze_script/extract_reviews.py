from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date
import argparse
from datetime import datetime
import sys
import os
from sql_reader.sql_reader import read_sql_file

try:
    LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL")
    HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL")
    STEAM_REVIEWS_MONGO_URI = os.getenv("STEAM_REVIEWS_MONGO_URI")

    if not LAKEHOUSE_URL or not HIVE_METASTORE_URL or not STEAM_REVIEWS_MONGO_URI:
        raise ValueError("Environment variables LAKEHOUSE_URL, HIVE_METASTORE_URL, and STEAM_REVIEWS_MONGO_URI must be set.")
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)


# Parse extraction day
try:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="Extraction Date in format YYYY-MM-DD")
    args = parser.parse_args()
    extraction_day = args.day
except argparse.ArgumentError as e:
    print(f"Day argument parsing error: {e}")
    sys.exit(1)

# Validate date format
try:
    extraction_day = datetime.strptime(extraction_day, "%Y-%m-%d")
except ValueError:
    print(f"Invalid date format: '{extraction_day}'. Expected format: YYYY-MM-DD.")
    sys.exit(1)    

try:
    spark = SparkSession.builder \
        .appName("SteamReviewByDay") \
        .config("spark.mongodb.read.connection.uri", STEAM_REVIEWS_MONGO_URI) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.steam_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.steam_catalog.type", "hive") \
        .config("spark.sql.catalog.steam_catalog.uri", HIVE_METASTORE_URL) \
        .config("spark.sql.catalog.steam_catalog.warehouse", LAKEHOUSE_URL) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
except Exception as e:
    print("Failed to create Spark session:")
    print(e)
    sys.exit(1)

# Try reading a sample from MongoDB
try:
    df = spark.read \
    .format("mongodb") \
    .option("database", "docuteDB") \
    .option("collection", "reviews") \
    .load()
except Exception as e:
    print("Connection failed:")
    print(e)
    sys.exit(1)
else:
    print("Connection successful.")


try:
    df = df.withColumn("created_day", to_date(from_unixtime("timestamp_created")))
except Exception as e:
    print("Error processing DataFrame:")
    print(e)
    sys.exit(1)

try:
    df = df.withColumn("created_day", to_date(df["created_day"]))
    df.createOrReplaceTempView("reviews")
except Exception as e:
    print("Error creating temporary reviews view:")
    print(e)
    sys.exit(1)

extract_reviews_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/reviews/extract_reviews.sql',
    extraction_day=extraction_day.strftime("%Y-%m-%d")
)

# SQL to create the bronze database
create_db_bronze_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/bronze_database/create_db_bronze.sql'
    )
print("Executing SQL:\n", create_db_bronze_sql)

try:
    spark.sql(create_db_bronze_sql)
except Exception as e:
    print(f"Error creating bronze database: {e}")
    sys.exit(1)

# Create the steam_reviews table in the bronze database
create_table_bronze_reviews_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/reviews/create_table_bronze_reviews.sql',
    LAKEHOUSE_URL=LAKEHOUSE_URL
)
print(create_table_bronze_reviews_sql)

try:
    spark.sql(create_table_bronze_reviews_sql)
except Exception as e:
    print("Error creating steam_reviews table in bronze database:")
    print(e)
    sys.exit(1)

extract_reviews_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/reviews/extract_reviews.sql',
    extraction_day=extraction_day
)
print(extract_reviews_sql)
try:
    spark.sql(extract_reviews_sql)
except Exception as e:
    print("Error merging data into steam_reviews table:")
    print(e)
    sys.exit(1)

spark.stop()
print("Data extraction and loading completed successfully.")
