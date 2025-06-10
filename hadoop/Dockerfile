FROM openjdk:8

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN apt-get update

RUN apt-get install -y ssh curl

RUN curl -O https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN tar -xzxf hadoop-$HADOOP_VERSION.tar.gz -C /opt/

RUN mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME

RUN rm hadoop-$HADOOP_VERSION.tar.gz

# Setup ssh
RUN ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

COPY config/* $HADOOP_HOME/etc/hadoop/

RUN echo 'export JAVA_HOME=/usr/local/openjdk-8' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$PATH:$JAVA_HOME/bin
