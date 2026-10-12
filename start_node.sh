#!/bin/bash
# start_node.sh - Run one broker and one storage node on this machine (multi-machine setup).
#
# Run it on every machine with the same STORAGE_NODES and BROKER_PEERS and a
# different BROKER_ID. Use at least 3 machines: Raft needs a majority of
# brokers to elect a leader, and each partition needs a majority of its
# replicas to accept writes, so 3 tolerates any one failure.
#
#   BROKER_ID=b1 ./start_node.sh
#
# Replace the IP addresses as appropriate.
cd "$(dirname "$0")"
mkdir -p data logs

: "${BROKER_ID:?Set BROKER_ID, e.g. BROKER_ID=b1}"
export BROKER_ID
export STORAGE_NODES="${STORAGE_NODES:-http://10.145.52.214:8001,http://10.5.16.156:8001,http://10.5.16.157:8001}"
export BROKER_PEERS="${BROKER_PEERS:-b1=http://10.145.52.214:8000,b2=http://10.5.16.156:8000,b3=http://10.5.16.157:8000}"

echo "Broker $BROKER_ID"
echo "  STORAGE_NODES: $STORAGE_NODES"
echo "  BROKER_PEERS:  $BROKER_PEERS"

STORAGE_DATA_DIR="data/storage" python -m uvicorn storage.server:app --host 0.0.0.0 --port 8001 &
STORAGE_PID=$!
BROKER_DATA_DIR="data/$BROKER_ID" python -m uvicorn broker.server:app --host 0.0.0.0 --port 8000 &
BROKER_PID=$!
trap 'kill $STORAGE_PID $BROKER_PID 2>/dev/null' EXIT

echo "Broker on :8000, storage on :8001. Run 'python ui/app.py' (set BROKER_URLS) to use it."
wait
