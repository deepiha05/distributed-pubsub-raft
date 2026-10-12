# Distributed Pub/Sub with Raft

A distributed publish-subscribe messaging system in Python (FastAPI). It runs as a cluster of **brokers** and **storage nodes**:

- **Brokers** keep the cluster metadata (topics, subscriptions, consumer offsets) consistent with **Raft consensus**. They handle leader election, log replication, majority commit, persistence, and CheckQuorum. Clients can talk to any broker, and followers forward requests to the leader.
- **Storage nodes** hold the partitioned message logs. Each partition is replicated across nodes, and a write needs a majority of its replicas, so a 3-node cluster survives any single failure.

Features: topics with partitions and retention, subscriptions with consumer groups, pull/ack/seek, dead-letter policies, and a CLI client.

## Project layout

```
broker/    Broker API, Raft implementation, metadata store
storage/   Storage node API and partition log engine
ui/        Interactive command-line client
tests/     End-to-end crash tests and network-partition tests
```

## Running

```bash
pip install -r requirements.txt
```

**Single machine.** This starts 3 brokers (ports 8000/8010/8020), 3 storage nodes (8001/8011/8021), and the CLI:

```bash
./start.sh
```

Data goes to `./data` and logs to `./logs`. Delete `./data` to start a fresh cluster.

**Multiple machines.** Set `STORAGE_NODES` and `BROKER_PEERS` to your machines' addresses, then run this on each machine (at least 3):

```bash
BROKER_ID=b1 ./start_node.sh
```

To use the CLI against a running cluster:

```bash
BROKER_URLS=http://host1:8000,http://host2:8000 python ui/app.py
```

## Tests

```bash
python tests/e2e.py        # crash/fault-injection tests (kills nodes with SIGKILL)
python tests/partition.py  # network-partition tests via in-process TCP proxies
```

## Limitations

Raft log compaction (snapshots) and cluster membership changes are not implemented.
