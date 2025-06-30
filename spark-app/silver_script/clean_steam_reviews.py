from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date
import argparse
from datetime import datetime
import sys
import os
from sql_reader.sql_reader import read_sql_file
import traceback

try:
    LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL", "hdfs://hadoop-master:9000/lakehouse")
    HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL", "thrift://hive-metastore:9083")
    if not LAKEHOUSE_URL or not HIVE_METASTORE_URL:
        raise ValueError("Environment variables LAKEHOUSE_URL and HIVE_METASTORE_URL must be set.")
except ValueError as e:
    print(f"Error when retrieving environment variables: {e}")
    sys.exit(1)
# Parse extraction day
try:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="Extraction Date in format YYYY-MM-DD")
    args = parser.parse_args()
    cleaning_day = args.day
except argparse.ArgumentError as e:
    print(f"Day argument parsing error: {e}")
    sys.exit(1)

# Validate date format
try:
    cleaning_day = datetime.strptime(cleaning_day, "%Y-%m-%d").strftime("%Y-%m-%d")
except ValueError:
    print(f"Invalid date format: '{cleaning_day}'. Expected format: YYYY-MM-DD.")
    sys.exit(1)


spark = SparkSession.builder \
    .appName("CleanSteamReviews") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.steam_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.steam_catalog.type", "hive") \
    .config("spark.sql.catalog.steam_catalog.uri", HIVE_METASTORE_URL) \
    .config("spark.sql.catalog.steam_catalog.warehouse", LAKEHOUSE_URL) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# SQL to create the silver database
create_db_silver_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/silver_database/create_db_silver.sql'
    )
print("Executing SQL:\n", create_db_silver_sql)

try:
    spark.sql(create_db_silver_sql)
except Exception as e:
    print(f"Error creating silver database: {e}")
    traceback.print_exc()
    sys.exit(1)

# SQL to create the steam_reviews table
create_table_silver_reviews_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/reviews/create_table_silver_reviews.sql',
    LAKEHOUSE_URL=LAKEHOUSE_URL
)
try:
    spark.sql(create_table_silver_reviews_sql)
except Exception as e:
    print(f"Error creating silver.reviews table: {e}")
    sys.exit(1)

# SQL to clean and merge reviews data
clean_reviews_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/reviews/clean_reviews.sql',
    cleaning_day=cleaning_day
)

try:
    spark.sql(clean_reviews_sql)
except Exception as e:
    print(f"Error cleaning and merging reviews data: {e}")
    sys.exit(1)

spark.stop()

print("Silver reviews table created and cleaned successfully.")
