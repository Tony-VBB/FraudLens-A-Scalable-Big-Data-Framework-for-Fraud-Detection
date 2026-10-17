#!/usr/bin/env bash
set -euo pipefail

PROJECT="$HOME/FraudLens"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Configuration not found: $CONFIG"
    echo "Run setup/configure_environment.sh first."
    exit 1
fi

# Load environment variables
source "$CONFIG"

echo "========== FraudLens Installation Verification =========="

FAILURES=0

check_command() {
    local name="$1"
    local command_path="$2"

    if [[ -x "$command_path" ]]; then
        printf 'OK: %s -> %s\n' "$name" "$command_path"
    else
        printf 'MISSING: %s -> %s\n' "$name" "$command_path"
        FAILURES=$((FAILURES + 1))
    fi
}

check_command "Java" "$JAVA_HOME/bin/java"
check_command "Hadoop" "$HADOOP_HOME/bin/hadoop"
check_command "HDFS" "$HADOOP_HOME/bin/hdfs"
check_command "Pig" "$PIG_HOME/bin/pig"
check_command "Hive Beeline" "$HIVE_HOME/bin/beeline"
check_command "Spark" "$SPARK_HOME/bin/spark-submit"

echo
echo "========== Java Version =========="
"$JAVA_HOME/bin/java" -version 2>&1 || FAILURES=$((FAILURES + 1))

echo
echo "========== Python Dependencies =========="
if python3.12 -c \
    'import numpy, packaging; print("NumPy:", numpy.__version__); print("packaging:", packaging.__version__)'
then
    :
else
    echo "ERROR: Python dependency check failed."
    FAILURES=$((FAILURES + 1))
fi

echo
echo "========== Spark Version =========="
if "$SPARK_HOME/bin/spark-submit" --version 2>&1; then
    :
else
    echo "ERROR: Spark version check failed."
    FAILURES=$((FAILURES + 1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "RESULT: All required executable and Python checks passed."
else
    echo "RESULT: $FAILURES check(s) failed. Resolve these before running the pipeline."
    exit 1
fi
