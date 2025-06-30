spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-app/silver_script/clean_steam_games.py