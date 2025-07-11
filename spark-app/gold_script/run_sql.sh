spark-sql \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.warehouse.dir=$LAKEHOUSE_URL \

