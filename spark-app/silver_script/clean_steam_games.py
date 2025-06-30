from pyspark.sql import SparkSession
import sys
import os
from sql_reader.sql_reader import read_sql_file
try:
    LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL")
    HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL")
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

try:
    spark = SparkSession.builder \
        .appName("CleanSteamGame") \
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

# SQL to create the silver database
create_db_silver_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/silver_database/create_db_silver.sql'
    )
print("Executing SQL:\n", create_db_silver_sql)

try:
    spark.sql(create_db_silver_sql)
except Exception as e:
    print(f"Error creating silver database: {e}")
    sys.exit(1)

# SQL to create the games table
create_table_silver_games_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/games/create_table_silver_games.sql',
    LAKEHOUSE_URL=LAKEHOUSE_URL
)
try:
    spark.sql(create_table_silver_games_sql)
except Exception as e:
    print(f"Error creating silver.games table: {e}")
    sys.exit(1)

# SQL to clean and merge games data
clean_games_sql = read_sql_file(
    '/opt/spark-app/silver_script/sql_clean/games/clean_games.sql'
)

try:
    spark.sql(clean_games_sql)
except Exception as e:
    print(f"Error cleaning and merging games data: {e}")
    sys.exit(1)

spark.stop()

print("Silver games table created and cleaned successfully.")