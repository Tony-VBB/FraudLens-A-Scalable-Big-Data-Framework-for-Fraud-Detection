#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT_HOME:-$HOME/FraudLens}"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Configuration file not found: $CONFIG"
    exit 1
fi

source "$CONFIG"

export JAVA_HOME
export HADOOP_HOME
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3.12}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3.12}"

if ! hdfs dfs -test -e "$HDFS_CLEAN_DIR/part-r-00000"; then
    echo "ERROR: Cleaned HDFS data not found: $HDFS_CLEAN_DIR"
    exit 1
fi

for path in "$HDFS_PREDICTIONS_DIR" "$HDFS_MODEL_DIR"; do
    if hdfs dfs -test -e "$path"; then
        echo "ERROR: Output already exists: $path"
        echo "Existing results were not deleted."
        exit 1
    fi
done

mkdir -p "$PROJECT/spark/results"

echo "Spark version:"
"$SPARK_HOME/bin/spark-submit" --version

echo
echo "Starting FraudLens Part 3..."

"$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --conf "spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS" \
    "$PROJECT/spark/fraud_model.py" \
    2>&1 | tee "$PROJECT/spark/results/spark_run.log"
