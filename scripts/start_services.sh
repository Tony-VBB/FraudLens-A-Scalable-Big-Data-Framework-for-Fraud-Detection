#!/usr/bin/env bash
set -euo pipefail

PROJECT="$HOME/FraudLens"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Configuration missing: $CONFIG"
    exit 1
fi

source "$CONFIG"

echo "========== FraudLens Service Startup =========="

# Validate Hadoop configuration before starting services.
if [[ ! -f "$HADOOP_CONF_DIR/core-site.xml" ||
      ! -f "$HADOOP_CONF_DIR/hdfs-site.xml" ]]; then
    echo "ERROR: Hadoop configuration files are missing."
    exit 1
fi

echo "Checking HDFS..."

if ! hdfs dfs -test -e /; then
    echo "Starting HDFS..."
    "$HADOOP_HOME/sbin/start-dfs.sh"
else
    echo "HDFS is already responding."
fi

echo "Checking YARN..."

if ! yarn node -list >/dev/null 2>&1; then
    echo "Starting YARN..."
    "$HADOOP_HOME/sbin/start-yarn.sh"
else
    echo "YARN appears to be responding."
fi

echo
echo "Checking HiveServer2..."

if timeout 3 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
    echo "HiveServer2 is already listening on port 10000."
else
    echo "Starting Hive metastore if needed..."
    if ! timeout 3 bash -c '</dev/tcp/localhost/9083' 2>/dev/null; then
        nohup "$HIVE_HOME/bin/hive" --service metastore \
            > "$PROJECT/logs/hive-metastore.log" 2>&1 &
    fi

    echo "Starting HiveServer2..."
    nohup "$HIVE_HOME/bin/hive" --service hiveserver2 \
        > "$PROJECT/logs/hiveserver2.log" 2>&1 &
fi

echo
echo "Waiting for HiveServer2..."
for attempt in $(seq 1 30); do
    if timeout 2 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
        echo "HiveServer2 is listening on port 10000."
        exit 0
    fi
    sleep 2
done

echo "ERROR: HiveServer2 did not become available within 60 seconds."
echo "Review logs in $PROJECT/logs."
exit 1
