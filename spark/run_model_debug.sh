#!/usr/bin/env bash

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$HOME/spark-3.5.9-bin-hadoop3"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON=python3.12
export PYSPARK_DRIVER_PYTHON=python3.12

LOG="$HOME/FraudLens/spark/results/spark_debug.log"

echo "FraudLens Spark debug run started: $(date)"
echo "Spark: $SPARK_HOME"
echo "Python: $(python3.12 --version 2>&1)"
echo "Hadoop filesystem: $("$HADOOP_HOME/bin/hdfs" getconf -confKey fs.defaultFS)"
echo "Log file: $LOG"
echo

"$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=8 \
  "$HOME/FraudLens/spark/fraud_model.py" \
  2>&1 | tee "$LOG"

STATUS=${PIPESTATUS[0]}
echo
echo "Spark exit code: $STATUS"
echo "Full log: $LOG"
echo "Press Enter to close this run..."
read -r
exit "$STATUS"
