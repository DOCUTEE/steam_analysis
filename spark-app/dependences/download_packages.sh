mkdir -p /opt/spark/jars

# Iceberg
wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar

# Mongo Spark Connector
wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar

wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar

wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar

# MongoDB Driver Core
wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar

