#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <warehouse_subdir> <sql_file>"
  exit 1
fi

WAREHOUSE_SUBDIR=$1
SQL_FILE=$2

if [ ! -f "$SQL_FILE" ]; then
  echo "SQL file $SQL_FILE does not exist!"
  exit 2
fi

WAREHOUSE_DIR="hdfs://hadoop-master:9000/$WAREHOUSE_SUBDIR"

spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$WAREHOUSE_DIR" \
  --conf spark.sql.defaultCatalog=local \
  -f "$SQL_FILE"
