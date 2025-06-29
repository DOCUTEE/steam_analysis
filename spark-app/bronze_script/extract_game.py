from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("ExtractGameFromMongoDB") \
    .config("spark.mongodb.read.connection.uri", "mongodb+srv://hyngnamwork:Hyngulem0812@cluster0.ntskfqd.mongodb.net/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.steam_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.steam_catalog.type", "hive") \
    .config("spark.sql.catalog.steam_catalog.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.steam_catalog.warehouse", "hdfs://hadoop-master:9000/lakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.format("mongodb") \
        .option("database", "steam") \
        .option("collection", "game_small") \
        .load()

    df.createOrReplaceTempView("game_small")
except Exception as e:
    print("Error reading from MongoDB:")
    print(e)
    sys.exit(1)

spark.sql("CREATE DATABASE IF NOT EXISTS steam_catalog.bronze")


spark.sql("""
    CREATE TABLE IF NOT EXISTS steam_catalog.bronze.game_small (
        appid INT,
        categories ARRAY<STRING>,
        created_at DOUBLE,
        detailed_description STRING,
        developers ARRAY<STRING>,
        genres ARRAY<STRING>,
        is_free BOOLEAN,
        platforms STRUCT<
            windows: BOOLEAN, 
            mac: BOOLEAN, 
            linux: BOOLEAN
        >,
        price_overview STRUCT<
            currency: STRING,
            initial: DOUBLE,
            final: DOUBLE,
            discount_percent: INT,
            initial_formatted: STRING,
            final_formatted: STRING
        >,
        publishers ARRAY<STRING>,
        release_date STRUCT<coming_soon: BOOLEAN, date: STRING>,
        required_age INT,
        short_description STRING,
        type STRING
    )
    USING iceberg
    LOCATION 'hdfs://hadoop-master:9000/lakehouse/bronze.db/game_small'
    TBLPROPERTIES (
        'format-version' = '2'
    );
    """
)

spark.sql("""
MERGE INTO steam_catalog.bronze.game_small AS target
USING (
    SELECT
        appid,
        categories,
        created_at,
        detailed_description,
        developers,
        genres,
        is_free,
        platforms,
        STRUCT(
            price_overview.currency AS currency,
            CAST(price_overview.initial AS DOUBLE) AS initial,
            CAST(price_overview.final AS DOUBLE) AS final,
            CAST(price_overview.discount_percent AS INT) AS discount_percent,
            price_overview.initial_formatted AS initial_formatted,
            price_overview.final_formatted AS final_formatted
        ) AS price_overview,
        publishers,
        release_date,
        CAST(required_age AS INT) AS required_age,
        short_description,
        type
    FROM game_small
) AS source
ON target.appid = source.appid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")



spark.stop()
