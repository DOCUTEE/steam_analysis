spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-app/bronze_script/extract_game.py