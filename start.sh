#!/bin/bash
# start.sh - Run a whole cluster on this machine: 3 brokers + 3 storage nodes, then the UI.
#
#   broker b1 :8000   storage s1 :8001
#   broker b2 :8010   storage s2 :8011
#   broker b3 :8020   storage s3 :8021
#
# Data is kept under ./data (delete it for a fresh cluster), logs under ./logs.
# To spread the cluster over several machines, use start_node.sh on each one.
cd "$(dirname "$0")"
mkdir -p data logs

export STORAGE_NODES="http://localhost:8001,http://localhost:8011,http://localhost:8021"
export BROKER_PEERS="b1=http://localhost:8000,b2=http://localhost:8010,b3=http://localhost:8020"

PIDS=()
cleanup() {
    echo "Stopping cluster..."
    kill "${PIDS[@]}" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

for i in 1 2 3; do
    port=$((8001 + (i - 1) * 10))
    STORAGE_DATA_DIR="data/storage$i" python -m uvicorn storage.server:app \
        --host 0.0.0.0 --port "$port" > "logs/storage$i.log" 2>&1 &
    PIDS+=($!)
done

for i in 1 2 3; do
    port=$((8000 + (i - 1) * 10))
    BROKER_ID="b$i" BROKER_DATA_DIR="data/b$i" python -m uvicorn broker.server:app \
        --host 0.0.0.0 --port "$port" > "logs/broker$i.log" 2>&1 &
    PIDS+=($!)
done

echo "Waiting for a leader to be elected..."
for _ in $(seq 1 60); do
    if curl -sf http://localhost:8000/topics > /dev/null; then
        break
    fi
    sleep 0.5
done
curl -s http://localhost:8000/health; echo

export BROKER_URLS="http://localhost:8000,http://localhost:8010,http://localhost:8020"
python ui/app.py
