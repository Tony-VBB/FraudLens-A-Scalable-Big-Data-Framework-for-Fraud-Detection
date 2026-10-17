#!/usr/bin/env bash
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="$PROJECT/config"
mkdir -p "$CONFIG_DIR"

find_java8() {
    local candidate
    for candidate in \
        "${JAVA_HOME:-}" \
        "$HOME/jdk8" \
        /usr/lib/jvm/java-8-openjdk-amd64 \
        /usr/lib/jvm/java-8-openjdk \
        /usr/lib/jvm/temurin-8-jdk-amd64
    do
        if [[ -n "$candidate" && -x "$candidate/bin/java" ]]; then
            if "$candidate/bin/java" -version 2>&1 | grep -q '1\.8'; then
                printf '%s\n' "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

find_tool() {
    local variable="$1" candidate="$2" executable="$3"
    local value="${!variable:-$candidate}"
    if [[ -x "$value/bin/$executable" ]]; then
        printf '%s\n' "$value"
    else
        echo "ERROR: Cannot find $executable under: $value" >&2
        echo "Install the software or set $variable to its installation directory." >&2
        return 1
    fi
}

JAVA_PATH="$(find_java8)" || {
    echo "ERROR: Java 8 was not found. Install Java 8 and retry."
    exit 1
}

HADOOP_PATH="$(find_tool HADOOP_HOME "$HOME/hadoop" hdfs)"
PIG_PATH="$(find_tool PIG_HOME "$HOME/pig-0.17.0" pig)"
HIVE_PATH="$(find_tool HIVE_HOME "$HOME/hive" beeline)"
SPARK_PATH="$(SPARK_HOME= find_tool SPARK_HOME "$HOME/spark-3.5.9-bin-hadoop3" spark-submit)"

cat > "$CONFIG_DIR/fraudlens.env" <<EOFCONFIG
# Generated for this machine by setup/configure_environment.sh
export PROJECT_HOME="$PROJECT"
export JAVA_HOME="$JAVA_PATH"
export HADOOP_HOME="$HADOOP_PATH"
export HADOOP_CONF_DIR="\$HADOOP_HOME/etc/hadoop"
export PIG_HOME="$PIG_PATH"
export HIVE_HOME="$HIVE_PATH"
export SPARK_HOME="$SPARK_PATH"
export PATH="\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PIG_HOME/bin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$PATH"
export PYSPARK_PYTHON=python3.12
export PYSPARK_DRIVER_PYTHON=python3.12
export HDFS_RAW_DIR=/fraudlens/raw
export HDFS_CLEAN_DIR=/fraudlens/processed/paysim_cleaned
export HDFS_MODEL_DIR=/fraudlens/models/spark_fraud_model
export HDFS_PREDICTIONS_DIR=/fraudlens/results/spark_fraud_predictions
export DATASET_FILENAME=PS_20174392719_1491204439457_log.csv
export SPARK_MASTER='local[2]'
export SPARK_DRIVER_MEMORY=2g
export SPARK_SHUFFLE_PARTITIONS=8
export HIVE_JDBC_URL=jdbc:hive2://localhost:10000/default
EOFCONFIG

echo "Configuration created: $CONFIG_DIR/fraudlens.env"
echo "Detected installations:"
echo "Java:   $JAVA_PATH"
echo "Hadoop: $HADOOP_PATH"
echo "Pig:    $PIG_PATH"
echo "Hive:   $HIVE_PATH"
echo "Spark:  $SPARK_PATH"
echo
echo "Run: source \"$CONFIG_DIR/fraudlens.env\""
