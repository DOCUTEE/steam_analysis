#!/bin/bash

# This script starts a Spark master
$SPARK_HOME/sbin/start-master.sh 

# Tail the master logs to keep the container running and to view logs in real-time.
tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master*.out