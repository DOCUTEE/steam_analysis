FROM openjdk:8-jdk

RUN apt-get update

RUN apt-get install -y \
    wget \
    curl \
    python3 \
    python3-pip \
    tar \
    netcat \
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

RUN apt-get update && apt-get install -y openssh-server

RUN mkdir -p /var/run/sshd

RUN useradd -m airflow_user && echo "airflow_user:airflow" | chpasswd

RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

ENV JAVA_HOME=/usr/local/openjdk-8

EXPOSE 22

# Set working directory
WORKDIR $SPARK_HOME

COPY ./conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./conf/spark-env.sh $SPARK_HOME/conf/spark-env.sh
