from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import sys
import os
from sql_reader.sql_reader import read_sql_file
import argparse
from datetime import datetime
import requests
import time

try:
    LAKEHOUSE_URL = os.getenv("LAKEHOUSE_URL")
    HIVE_METASTORE_URL = os.getenv("HIVE_METASTORE_URL")
    STEAM_GAMES_MONGO_URI = os.getenv("STEAM_GAMES_MONGO_URI")
    if not LAKEHOUSE_URL or not HIVE_METASTORE_URL:
        raise ValueError("Environment variables LAKEHOUSE_URL and HIVE_METASTORE_URL must be set.")
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
    
extract_appid_sql = read_sql_file(
    '/opt/spark-app/bronze_script/sql_extract_load/games/extract_appids.sql',
    extraction_day=extraction_day.strftime("%Y-%m-%d")
)

try:
    df = spark.sql(extract_appid_sql)
except Exception as e:
    print("Error extracting appid from reviews:")
    print(e)
    sys.exit(1)
    
appids = [row.appid for row in df.collect()]
print(appids)


# Gọi Steam API
def fetch_game_data(appid: int):
    try:
        url = "https://store.steampowered.com/api/appdetails"
        params = {"appids": appid}
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json()
        if str(appid) in result and result[str(appid)].get("success"):
            return appid, result[str(appid)]["data"]
    except Exception as e:
        print(f"Failed to fetch appid {appid}: {e}")
    return appid, None

# Duyệt qua appids và gọi API
records = []
for appid in appids:
    appid, data = fetch_game_data(appid)
    if data:
        try:
            records.append({
                "appid": appid,
                "name": data.get("name", ""),
                "type": data.get("type", ""),
                "is_free": data.get("is_free", False),
                "developers": data.get("developers", []),
                "publishers": data.get("publishers", []),
                "genres": [g.get("description", "") for g in data.get("genres", [])],
                "categories": [c.get("description", "") for c in data.get("categories", [])],
                "price_overview": {
                    "currency": data.get("price_overview", {}).get("currency", ""),
                    "initial": float(data.get("price_overview", {}).get("initial", 0)) / 100,
                    "final": float(data.get("price_overview", {}).get("final", 0)) / 100,
                    "discount_percent": int(data.get("price_overview", {}).get("discount_percent", 0)),
                    "initial_formatted": data.get("price_overview", {}).get("initial_formatted", ""),
                    "final_formatted": data.get("price_overview", {}).get("final_formatted", "")
                },
                "release_date": {
                    "coming_soon": data.get("release_date", {}).get("coming_soon", False),
                    "date": data.get("release_date", {}).get("date", "")
                },
                "required_age": int(data.get("required_age", 0) or 0),
                "short_description": data.get("short_description", ""),
                "detailed_description": data.get("detailed_description", ""),
                "platforms": {
                    "windows": data.get("platforms", {}).get("windows", False),
                    "mac": data.get("platforms", {}).get("mac", False),
                    "linux": data.get("platforms", {}).get("linux", False),
                },
                "created_at": time.time()
            })
            print(f"Fetched {appid}: {data.get('name')}")
        except Exception as e:
            print(f"Failed to parse data for appid {appid}: {e}")
    time.sleep(1)  # Delay tránh rate limit
    
schema = StructType([
    StructField("appid", IntegerType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("is_free", BooleanType()),
    StructField("developers", ArrayType(StringType())),
    StructField("publishers", ArrayType(StringType())),
    StructField("genres", ArrayType(StringType())),
    StructField("categories", ArrayType(StringType())),
    StructField("price_overview", StructType([
        StructField("currency", StringType()),
        StructField("initial", DoubleType()),
        StructField("final", DoubleType()),
        StructField("discount_percent", IntegerType()),
        StructField("initial_formatted", StringType()),
        StructField("final_formatted", StringType())
    ])),
    StructField("release_date", StructType([
        StructField("coming_soon", BooleanType()),
        StructField("date", StringType())
    ])),
    StructField("required_age", IntegerType()),
    StructField("short_description", StringType()),
    StructField("detailed_description", StringType()),
    StructField("platforms", StructType([
        StructField("windows", BooleanType()),
        StructField("mac", BooleanType()),
        StructField("linux", BooleanType())
    ])),
    StructField("created_at", DoubleType())
])

games_df = spark.createDataFrame([Row(**r) for r in records], schema)
games_df.createOrReplaceTempView("new_games")

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
