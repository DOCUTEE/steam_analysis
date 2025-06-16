from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoToIceberg") \
    .config("spark.mongodb.read.connection.uri", "mongodb://NamHy:NamHyCute@host.docker.internal:27017/steam_db?authSource=admin") \
    .config("spark.mongodb.read.database", "steam_db") \
    .config("spark.mongodb.read.collection", "steam_review") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "hdfs://hadoop-master:9000/user/hive/warehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# Đọc từ MongoDB
df = spark.read.format("mongodb").load()

# Ghi vào bảng Iceberg trong Hadoop Catalog
df.writeTo("my_catalog.testdb.my_table").createOrReplace()
