from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date
import argparse
from datetime import datetime
import sys




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

spark = SparkSession.builder \
    .appName("SteamReviewByDay") \
    .config("spark.mongodb.read.connection.uri", "mongodb://NamHy:NamHyCute@host.docker.internal:27017/steam_db?authSource=admin&tls=false") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.steam_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.steam_catalog.type", "hive") \
    .config("spark.sql.catalog.steam_catalog.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.steam_catalog.warehouse", "hdfs://hadoop-master:9000/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


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
    sys.exit(1)
else:
    print("Connection successful.")



df = df.withColumn("created_day", to_date(from_unixtime("timestamp_created")))

df.createOrReplaceTempView("reviews")

steam_reviews = spark.sql(
    f'''
    SELECT 
        recommendationid,
        appid,
        game,
        author_steamid,
        author_num_games_owned,
        author_num_reviews,
        author_playtime_forever,
        author_playtime_last_two_weeks,
        author_playtime_at_review,
        author_last_played,
        language,
        review,
        timestamp_created,
        timestamp_updated,
        voted_up,
        votes_up,
        votes_funny,
        weighted_vote_score,
        comment_count,
        steam_purchase,
        received_for_free,
        written_during_early_access,
        hidden_in_steam_china,
        steam_china_location,
        created_day
    FROM reviews
    WHERE DATE(created_day) = DATE('{extraction_day}')
    '''
)

steam_reviews.createOrReplaceTempView("steam_reviews")

spark.sql("CREATE DATABASE IF NOT EXISTS steam_catalog.bronze")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS steam_catalog.bronze.steam_reviews (
        recommendationid INT,
        appid INT,
        game STRING,
        author_steamid LONG,
        author_num_games_owned INT,
        author_num_reviews INT,
        author_playtime_forever INT,
        author_playtime_last_two_weeks INT,
        author_playtime_at_review INT,
        author_last_played INT,
        language STRING,
        review STRING,
        timestamp_created INT,
        timestamp_updated INT,
        voted_up INT,
        votes_up INT,
        votes_funny INT,
        weighted_vote_score DOUBLE,
        comment_count INT,
        steam_purchase INT,
        received_for_free INT,
        written_during_early_access INT,
        hidden_in_steam_china INT,
        steam_china_location STRING,
        created_day DATE
    )
    USING iceberg
    PARTITIONED BY (created_day)
    LOCATION 'hdfs://hadoop-master:9000/lakehouse/bronze.db/steam_reviews'
    TBLPROPERTIES (
        'format-version' = '2'
    );
    """
)

spark.sql("""
    MERGE INTO steam_catalog.bronze.steam_reviews AS target
    USING steam_reviews AS source
    ON target.recommendationid = source.recommendationid
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
""")

