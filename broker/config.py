# broker/config.py
"""
Broker configuration, read from environment variables.

    BROKER_ID         this broker's id, e.g. "b1"
    BROKER_PEERS      every broker in the cluster, including this one:
                      "b1=http://10.0.0.1:8000,b2=http://10.0.0.2:8000,b3=http://10.0.0.3:8000"
    STORAGE_NODES     every storage node: "http://10.0.0.1:8001,http://10.0.0.2:8001,..."
    BROKER_DATA_DIR   where the Raft log is persisted (default data/<BROKER_ID>)
"""
import os

BROKER_ID = os.environ.get("BROKER_ID", "b1")


def _parse_peers():
    env = os.environ.get("BROKER_PEERS", "")
    peers = {}
    for item in filter(None, (s.strip() for s in env.split(","))):
        pid, _, url = item.partition("=")
        peers[pid.strip()] = url.strip().rstrip("/")
    if BROKER_ID not in peers:
        peers[BROKER_ID] = os.environ.get("BROKER_URL", "http://localhost:8000")
    return peers


BROKER_PEERS = _parse_peers()
STORAGE_NODES = [n.strip().rstrip("/") for n in
                 os.environ.get("STORAGE_NODES", "http://localhost:8001").split(",") if n.strip()]
DATA_DIR = os.environ.get("BROKER_DATA_DIR", os.path.join("data", BROKER_ID))
FSYNC = os.environ.get("BROKER_FSYNC", "true").lower() == "true"

ELECTION_TIMEOUT = (float(os.environ.get("ELECTION_TIMEOUT_MIN", "1.0")),
                    float(os.environ.get("ELECTION_TIMEOUT_MAX", "2.0")))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.2"))
MAINTENANCE_INTERVAL = float(os.environ.get("MAINTENANCE_INTERVAL", "5"))
