/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.warehouse.dir=$LAKEHOUSE_URL \
  --conf spark.sql.thriftServer.port=10000 \
  /opt/spark/jars/spark-hive-thriftserver_2.12-3.4.0.jar

# /opt/spark-app/gold_script/run_thrift.sh