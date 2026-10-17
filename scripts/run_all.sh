#!/usr/bin/env bash
set -euo pipefail

PROJECT="$HOME/FraudLens"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Missing configuration: $CONFIG"
    exit 1
fi

source "$CONFIG"

mkdir -p "$PROJECT/logs" "$PROJECT/spark/results"

LOG="$PROJECT/logs/run_all_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

echo "========== FraudLens Master Runner =========="
echo "Project: $PROJECT"
echo "Log: $LOG"
echo

# Check the existing pipeline files.
for file in \
    "$PROJECT/pig/clean_paysim.pig" \
    "$PROJECT/hive/fraud_analysis.hql" \
    "$PROJECT/spark/fraud_model.py" \
    "$PROJECT/spark/run_fraud_model.sh"
do
    if [[ ! -s "$file" ]]; then
        echo "ERROR: Required pipeline file is missing: $file"
        exit 1
    fi
done

# Check the configured software.
for executable in \
    "$JAVA_HOME/bin/java" \
    "$HADOOP_HOME/bin/hdfs" \
    "$HADOOP_HOME/bin/hadoop" \
    "$PIG_HOME/bin/pig" \
    "$HIVE_HOME/bin/beeline" \
    "$SPARK_HOME/bin/spark-submit"
do
    if [[ ! -x "$executable" ]]; then
        echo "ERROR: Required executable not found: $executable"
        exit 1
    fi
done

export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin:$HIVE_HOME/bin:$SPARK_HOME/bin:$PATH"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3.12}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3.12}"

DATASET="$PROJECT/data/raw/paysim/$DATASET_FILENAME"
RAW_HDFS="$HDFS_RAW_DIR/$DATASET_FILENAME"

if [[ ! -s "$DATASET" ]]; then
    echo "ERROR: Raw dataset missing: $DATASET"
    exit 1
fi

# Confirm HDFS is available before making changes.
if ! hdfs dfs -test -e /; then
    echo "ERROR: HDFS is unavailable. Start and verify Hadoop services first."
    exit 1
fi

echo "Checking HDFS inputs and outputs..."

# Upload raw data only if it does not already exist.
if hdfs dfs -test -e "$RAW_HDFS"; then
    echo "Raw HDFS dataset already exists; preserving it."
else
    echo "Uploading raw dataset to HDFS..."
    hdfs dfs -mkdir -p "$HDFS_RAW_DIR"
    hdfs dfs -put "$DATASET" "$RAW_HDFS"
fi

# Do not overwrite the cleaned output.
if hdfs dfs -test -e "$HDFS_CLEAN_DIR"; then
    echo "Cleaned data already exists. Preserving it; skipping Pig."
else
    echo "========== PART 1: PIG =========="
    "$PIG_HOME/bin/pig" "$PROJECT/pig/clean_paysim.pig"
fi

# Check whether HiveServer2 is reachable.
if ! timeout 5 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
    echo "ERROR: HiveServer2 is not listening on localhost:10000."
    echo "Start Hadoop, the Hive metastore, and HiveServer2 before retrying."
    exit 1
fi

HIVE_MARKER="$PROJECT/logs/hive_analysis_completed"

if [[ -f "$HIVE_MARKER" ]]; then
    echo "Hive completion marker exists; skipping Hive analysis."
elif [[ -s "$PROJECT/hive/fraud_analysis_output.log" ]] &&
     grep -q "7820" "$PROJECT/hive/fraud_analysis_output.log"; then
    echo "Existing Hive analysis output detected; preserving it and skipping Hive."
    echo "If you later rerun Hive intentionally, review its output paths first."
else
    echo "========== PART 2: HIVE =========="
    "$HIVE_HOME/bin/beeline"         -u "$HIVE_JDBC_URL"         -f "$PROJECT/hive/fraud_analysis.hql"         2>&1 | tee "$PROJECT/hive/fraud_analysis_output.log"
    touch "$HIVE_MARKER"
fi

# The existing Spark runner has its own path and output checks.
echo "========== PART 3: SPARK =========="
if hdfs dfs -test -e "$HDFS_MODEL_DIR" ||
   hdfs dfs -test -e "$HDFS_PREDICTIONS_DIR"; then
    echo "Existing Spark model or predictions found."
    echo "Preserving them; skipping Spark training."
else
    "$PROJECT/spark/run_fraud_model.sh"
fi

echo
echo "FraudLens master run finished."
echo "Log saved to: $LOG"
