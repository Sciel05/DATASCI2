#!/usr/bin/env bash
# run_spark_detector.sh - Runs the Spark Streaming Anomaly Detector

set -e

cd /mnt/c/Users/user/Downloads/DATASCI2

export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 pyspark-shell"
export CASSANDRA_HOST="127.0.0.1"
export CASSANDRA_CONSISTENCY="LOCAL_QUORUM"

echo "=== Starting Spark Structured Streaming Detector ==="
python3 stream-processing/detect_anomalies.py \
    --bootstrap-servers localhost:9092 \
    --input-topic market-ticks \
    --cassandra-sink
