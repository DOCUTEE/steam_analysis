#!/bin/bash

# This script starts a Spark worker and connects it to the Spark master.
$SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077 

# Tail the worker logs to keep the container running and to view logs in real-time.
tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.worker*.out