#!/bin/bash

# Чтобы предотвратить path rewriting от Git Bash
export MSYS_NO_PATHCONV=1

# === Параметры для главной job-ы ===
SPLIT_MB=${1:-10}
REDUCERS_COUNT=${2:-10}

HDFS_INPUT_DIR="/input"
HDFS_INTERMEDIATE_DIR="/tmp/pipeline-temp"
HDFS_OUTPUT_DIR="/output"

echo "=== Starting Hadoop containers ==="
docker compose -f ./hadoop/docker-compose.yml up -d

echo "=== Waiting for HDFS to leave safemode ==="
docker exec namenode hdfs dfsadmin -safemode wait

echo "=== Copying input CSV files to HDFS ==="
if docker exec namenode hdfs dfs -test -d /input; then
    echo "HDFS directory /input already exists"
else
    echo "Creating HDFS directory /input and put csv files"
    docker exec namenode hdfs dfs -mkdir /input
    docker exec namenode hdfs dfs -put -f /opt/hadoop/jobs/*.csv /input
fi

echo "=== HDFS input csv files ==="
docker exec namenode hdfs dfs -ls /input

echo "=== Building JAR ==="
./gradlew clean jar

echo "=== Copy JAR to jobs folder ==="
cp build/libs/sales-analyzer-job.jar ./jobs/sales-analyzer-job.jar

echo "=== Cleaning previous HDFS directories ==="
docker exec namenode hdfs dfs -rm -r -f "$HDFS_INTERMEDIATE_DIR"
docker exec namenode hdfs dfs -rm -r -f "$HDFS_OUTPUT_DIR"

echo "=== Running pipeline ==="
docker exec resourcemanager yarn jar \
    /opt/hadoop/jobs/sales-analyzer-job.jar \
      "$HDFS_INPUT_DIR" \
      "$HDFS_INTERMEDIATE_DIR" \
      "$HDFS_OUTPUT_DIR" \
      "$SPLIT_MB" \
      "$REDUCERS_COUNT"

echo "=== Final output ==="
docker exec namenode hdfs dfs -cat /output/part-r-00000 > result.txt

