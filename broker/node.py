# broker/node.py
"""Wires this broker's components together and runs leader-only maintenance."""
import logging
import threading

from broker import config
from broker.metadata import MetadataStore
from broker.raft import RaftNode, NotLeaderError
from broker.storage_client import StorageCluster

log = logging.getLogger("broker")

metadata = MetadataStore()
storage = StorageCluster(config.STORAGE_NODES)
raft = RaftNode(config.BROKER_ID, config.BROKER_PEERS, config.DATA_DIR, metadata.apply,
                election_timeout=config.ELECTION_TIMEOUT, heartbeat=config.HEARTBEAT_INTERVAL,
                fsync=config.FSYNC)


class DeliveryTracker:
    """Counts how many times each unacknowledged message has been delivered."""

    def __init__(self):
        self.lock = threading.Lock()
        self.counts = {}  # (subscription id, partition) -> {offset: attempts}

    def count(self, sid, partition, offset):
        with self.lock:
            return self.counts.get((sid, partition), {}).get(offset, 0)

    def bump(self, sid, partition, offset):
        with self.lock:
            per = self.counts.setdefault((sid, partition), {})
            per[offset] = per.get(offset, 0) + 1
            return per[offset]

    def acked(self, sid, partition, next_offset):
        with self.lock:
            per = self.counts.get((sid, partition), {})
            for off in [o for o in per if o < next_offset]:
                del per[off]


deliveries = DeliveryTracker()


def maintenance_loop(stop: threading.Event):
    """Leader only: enforce retention, re-replicate lagging replicas, delete orphaned data."""
    while not stop.wait(config.MAINTENANCE_INTERVAL):
        try:
            epoch = raft.leader_epoch()
        except NotLeaderError:
            continue
        try:
            for topic in metadata.list_topics():
                for p in range(topic["partitions"]):
                    storage.enforce_retention(topic, p)
                    storage.repair(topic, p, epoch)
            storage.collect_garbage(lambda: {t["id"] for t in metadata.list_topics()})
        except Exception:
            log.exception("Maintenance pass failed")
