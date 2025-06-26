#!/bin/bash

echo "Executing custom commands for Atlas Hadoop..."
# Execute custom Hadoop setup script for Atlas Hadoop
${HADOOP_HOME}/bin/hdfs dfs -chown -R rajveersingh:hadoop /tmp/hive /user/hive
