FROM openjdk:8-jdk

RUN apt-get update

RUN apt-get install -y \
    wget \
    curl \
    python3 \
    python3-pip \
    tar \
    nano

RUN apt-get clean

ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark 
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and extract Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install PySpark
RUN pip3 install pyspark==${SPARK_VERSION}

# Set working directory
WORKDIR $SPARK_HOME

COPY start.sh /start.sh
RUN chmod +x /start.sh

ENTRYPOINT ["/start.sh"]