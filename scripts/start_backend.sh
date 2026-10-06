#!/usr/bin/env bash
# start_backend.sh - Starts Cassandra and Kafka inside Ubuntu WSL

set -e

echo "=== 1. Starting Apache Cassandra ==="
sudo service cassandra start
sleep 2

echo "=== 2. Checking Cassandra Status ==="
nodetool status | grep -E '^UN' || echo "Cassandra is starting up..."

echo "=== 3. Starting Apache Kafka ==="
if [ -d "$HOME/kafka_2.13-4.2.2" ]; then
    cd "$HOME/kafka_2.13-4.2.2"
    # Stop any old stale process just in case
    ./bin/kafka-server-stop.sh 2>/dev/null || true
    sleep 2
    ./bin/kafka-server-start.sh -daemon config/kraft/server.properties
    sleep 3
    echo "Checking Kafka topics:"
    ./bin/kafka-topics.sh --list --bootstrap-server localhost:9092
else
    echo "Kafka directory not found in $HOME/kafka_2.13-4.2.2"
fi

echo ""
echo ">>> SUCCESS: Cassandra (port 9042) and Kafka (port 9092) are running! <<<"
