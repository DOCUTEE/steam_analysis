from pyspark.sql import SparkSession
import sys
import os
from sql_reader.sql_reader import read_sql_file

try:
    LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL")
    HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL")
    STEAM_GAMES_MONGO_URI = os.getenv("STEAM_GAMES_MONGO_URI")
    if not LAKEHOUSE_URL or not HIVE_METASTORE_URL:
        raise ValueError("Environment variables LAKEHOUSE_URL and HIVE_METASTORE_URL must be set.")
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

try:
    spark = SparkSession.builder \
        .appName("ExtractGameFromMongoDB") \
        .config("spark.mongodb.read.connection.uri", STEAM_GAMES_MONGO_URI) \
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

try:
    df = spark.read.format("mongodb") \
        .option("database", "steam") \
        .option("collection", "game_small") \
        .load()

    df.createOrReplaceTempView("original_games")
except Exception as e:
    print("Error reading from MongoDB:")
    print(e)
    sys.exit(1)

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

create_table_bronze_games_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/games/create_table_bronze_games.sql',
    LAKEHOUSE_URL=LAKEHOUSE_URL
)

print(create_table_bronze_games_sql)

try:
    spark.sql(create_table_bronze_games_sql)
except Exception as e:
    print("Error creating bronze games table:")
    print(e)
    sys.exit(1)

extract_games_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/games/extract_games.sql'
)
print(extract_games_sql)
try:
    spark.sql(extract_games_sql)
except Exception as e:
    print("Error creating games table in bronze database:")
    print(e)
    sys.exit(1)
    
spark.stop()

print("Game extraction completed successfully.")
