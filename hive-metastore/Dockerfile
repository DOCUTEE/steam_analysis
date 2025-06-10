FROM openjdk:8-jdk

ENV HIVE_VERSION=4.0.1
ENV HADOOP_VERSION=3.3.6
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

RUN apt-get update

RUN apt-get install -y wget curl netcat procps

RUN wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    mv apache-hive-${HIVE_VERSION}-bin /opt/hive && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

# MySQL JDBC driver
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.26/mysql-connector-java-8.0.26.jar \
    -P $HIVE_HOME/lib/

RUN mkdir -p /opt/hive/metastore_db

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz


