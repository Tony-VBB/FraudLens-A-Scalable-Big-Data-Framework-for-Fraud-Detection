#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT_HOME:-$HOME/FraudLens}"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Missing configuration: $CONFIG"
    echo "Run setup/configure_environment.sh first."
    exit 1
fi

source "$CONFIG"

LOCAL_CSV="$PROJECT/data/raw/paysim/$DATASET_FILENAME"
HDFS_CSV="$HDFS_RAW_DIR/$DATASET_FILENAME"

if [[ ! -s "$LOCAL_CSV" ]]; then
    echo "ERROR: Dataset not found or empty:"
    echo "$LOCAL_CSV"
    echo "Place the PaySim CSV at this location and try again."
    exit 1
fi

if hdfs dfs -test -e "$HDFS_CSV"; then
    echo "HDFS dataset already exists; preserving it:"
    echo "$HDFS_CSV"
else
    echo "Creating HDFS raw-data directory..."
    hdfs dfs -mkdir -p "$HDFS_RAW_DIR"
    echo "Uploading dataset..."
    hdfs dfs -put "$LOCAL_CSV" "$HDFS_CSV"
fi

echo "Data preparation check completed."
