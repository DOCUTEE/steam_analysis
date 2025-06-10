from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("ReadOnlyFromHDFS") \
    .getOrCreate()

# 2. Read CSV from HDFS
df = spark.read.option("header", "true").csv("hdfs://hadoop-master:9000/data/input/sample.csv")

# 3. Show a few rows to verify
df.show(5)

# 4. Done
spark.stop()
