spark-submit --jars /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-app/silver_script/clean_steam_reviews.py \
            --day $1

# --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \