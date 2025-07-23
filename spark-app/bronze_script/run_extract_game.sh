#!/bin/bash

spark-submit \
  --jars $(echo /opt/spark/jars/*.jar | tr ' ' ',') \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-app/bronze_script/extract_game.py \
  --day $1
