#!/bin/bash

# Start SSH
service ssh start

# Format HDFS if not already formatted
if [ ! -d /opt/hadoop/data/namenode/current ]; then
  hdfs namenode -format -force -nonInteractive
fi


# start Hadoop
start-dfs.sh

# Hold the container open
tail -f /dev/null
