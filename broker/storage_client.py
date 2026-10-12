# broker/storage_client.py
"""
How the leader broker talks to the storage nodes.

Each partition is replicated on `replica_factor` distinct storage nodes. The
leader broker assigns offsets, so every replica stores the same message at the
same offset, and a write succeeds once a majority of the partition's replicas
have persisted it (quorum write).

Before its first write to a partition (and after any failed write), the leader
"fences" the replicas with its epoch (Raft term) and starts a new generation.
Replicas then reject writes from any older leader. The leader resumes from the
most up-to-date replica among a majority, compared by (generation of its last
message, log end) as Raft compares logs, so every acknowledged message is kept.

Like Raft's AppendEntries, each write carries the id of the message that must
precede it; a replica whose history differs rejects it, and the leader then
overwrites the replica's divergent tail from an up-to-date replica.

A replica that missed writes (e.g. it was down) is caught up automatically:
on its next write it reports a gap and the leader copies the missing messages
from another replica, and a periodic repair pass does the same for idle
partitions.
"""
import hashlib
import itertools
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

log = logging.getLogger("broker.storage")

SYNC_WINDOW = 100  # how far back to compare a lagging replica's tail for divergence
SUSPECT_SECONDS = 2.0  # after a failed call, skip that node for this long
FETCH_BATCH = 500


class QuorumError(Exception):
    pass


class StaleEpochError(Exception):
    pass


def quorum(n):
    return n // 2 + 1


class StorageCluster:
    def __init__(self, nodes, timeout=2.0):
        self.nodes = list(nodes)
        self.timeout = timeout
        self.pool = ThreadPoolExecutor(max_workers=32)
        self._locks_lock = threading.Lock()
        self._locks = {}
        self.next_offset = {}  # (topic_id, partition) -> (epoch, next offset, prev id, gen)
        self._gen_seq = itertools.count(1)
        self._suspect = {}  # node -> time until which it is skipped after a failure

    # ----- plumbing -----

    def _lock(self, key):
        with self._locks_lock:
            return self._locks.setdefault(key, threading.Lock())

    def _call(self, method, url, **kwargs):
        """Returns (status, json) or None if the node is unreachable."""
        node = next((n for n in self.nodes if url.startswith(n + "/")), None)
        try:
            r = requests.request(method, url, timeout=self.timeout, **kwargs)
            self._suspect.pop(node, None)
            return r.status_code, r.json()
        except Exception as e:
            log.debug("%s %s failed: %s", method, url, e)
            if node:
                self._suspect[node] = time.monotonic() + SUSPECT_SECONDS
            return None

    def _parallel(self, fn, nodes):
        futures = {n: self.pool.submit(fn, n) for n in nodes}
        return {n: f.result() for n, f in futures.items()}

    def _until(self, fn, nodes, enough):
        """
        Run fn on every node in parallel and return as soon as enough(results) holds,
        so one slow or unreachable replica does not delay a quorum operation. Calls
        still running finish in the background.
        """
        futures = {self.pool.submit(fn, n): n for n in nodes}
        results = {}
        for f in as_completed(futures):
            results[futures[f]] = f.result()
            if enough(results):
                break
        return results

    def _healthy(self, nodes, need):
        """Nodes not recently failed, unless skipping them would leave fewer than `need`."""
        now = time.monotonic()
        up = [n for n in nodes if self._suspect.get(n, 0) <= now]
        return up if len(up) >= need else list(nodes)

    def _quorum_op(self, fn, nodes, need, enough):
        """
        Try the nodes that have not failed recently first; if that is not enough
        (a skipped node may have recovered while a tried one failed), try the rest.
        """
        first = self._healthy(nodes, need)
        results = self._until(fn, first, enough)
        rest = [n for n in nodes if n not in first]
        if not enough(results) and rest:
            results.update(self._until(fn, rest, lambda r: enough({**results, **r})))
        return results

    def replicas(self, topic, partition):
        """The storage nodes holding a partition: replica_factor distinct nodes."""
        n = len(self.nodes)
        rf = max(1, min(topic["replica_factor"], n))
        base = (int(hashlib.sha256(topic["id"].encode()).hexdigest(), 16) + partition) % n
        return [self.nodes[(base + i) % n] for i in range(rf)]

    @staticmethod
    def _url(node, topic_id, partition, suffix=""):
        return f"{node}/store/{topic_id}/{partition}{suffix}"

    # ----- writes -----

    def publish(self, topic, partition, message, epoch):
        tid = topic["id"]
        key = (tid, partition)
        replicas = self.replicas(topic, partition)
        q = quorum(len(replicas))
        with self._lock(key):
            _, offset, prev_id, gen = self._state(tid, partition, replicas, q, epoch)
            msg = dict(message, offset=offset, gen=gen)
            # Replicas skipped here are caught up later (gap on next write, or repair).
            results = self._quorum_op(
                lambda n: self._store(n, tid, partition, [msg], epoch, replicas, prev_id),
                replicas, q, lambda r: sum(1 for ok in r.values() if ok) >= q)
            acks = sum(1 for ok in results.values() if ok)
            if acks >= q:
                self.next_offset[key] = (epoch, offset + 1, msg["id"], gen)
                return {"offset": offset, "replicas_acked": acks, "replicas": len(replicas)}
            # Some replicas may have kept it. Reuse the offset with a newer generation, so the
            # next message overwrites it wherever it landed; reads never go past `offset`.
            self.next_offset[key] = (epoch, offset, prev_id, self._new_gen(epoch))
            raise QuorumError(f"Only {acks} of {len(replicas)} replicas stored the message "
                              f"(quorum is {q})")

    def _new_gen(self, epoch):
        """A generation later than every earlier one: by epoch, then by order on this leader."""
        return epoch * 1_000_000 + next(self._gen_seq) % 1_000_000

    def _state(self, tid, partition, replicas, q, epoch):
        """
        (epoch, committed end, id of the last committed message, generation) for this
        leader; fences the partition on first use in an epoch. Call with the partition
        lock held. Every offset below the committed end is stored on a majority.
        """
        key = (tid, partition)
        cached = self.next_offset.get(key)
        if cached is None or cached[0] != epoch:
            end, last_id = self._fence(tid, partition, replicas, q, epoch)
            cached = (epoch, end, last_id, self._new_gen(epoch))
            self.next_offset[key] = cached
        return cached

    def _fence(self, tid, partition, replicas, q, epoch):
        # Any majority suffices: it overlaps every majority that acknowledged a write.
        results = self._quorum_op(
            lambda n: self._call("POST", self._url(n, tid, partition, "/fence"), json={"epoch": epoch}),
            replicas, q, lambda r: sum(1 for x in r.values() if x is not None and x[0] == 200) >= q)
        tails_by_node = {}
        for node, res in results.items():
            if res is None:
                continue
            status, body = res
            if status == 409 and body.get("error") == "stale_epoch":
                raise StaleEpochError("A newer leader has taken over this partition")
            if status == 200:
                tails_by_node[node] = body
        if len(tails_by_node) < q:
            raise QuorumError(f"Only {len(tails_by_node)} of {len(replicas)} replicas reachable (quorum is {q})")
        best_node = max(tails_by_node, key=lambda n: (tails_by_node[n]["last_gen"], tails_by_node[n]["end"]))
        best = tails_by_node[best_node]
        # Adopt the most up-to-date history, but first make sure a majority holds it,
        # so nothing served from it can later be replaced (Raft commits an old leader's
        # entries only once they are on a majority).
        matching = 1
        for node, t in tails_by_node.items():
            if node == best_node:
                continue
            if (t["end"], t["last_id"]) == (best["end"], best["last_id"]) or \
                    self._sync_replica(node, tid, partition, best["end"], [best_node], epoch):
                matching += 1
        if matching < q:
            raise QuorumError(f"Could not bring a majority of replicas up to date ({matching} of {q})")
        return best["end"], best["last_id"]

    def _ranked(self, tid, partition, nodes):
        """Reachable nodes, most up-to-date first: by (generation of last message, log end)."""
        infos = self._parallel(lambda n: self._call("GET", self._url(n, tid, partition, "/offset")), nodes)
        ok = {n: r[1] for n, r in infos.items() if r is not None and r[0] == 200}
        return sorted(ok, key=lambda n: (ok[n]["last_gen"], ok[n]["end"]), reverse=True), ok

    def _store(self, node, tid, partition, msgs, epoch, replicas, prev_id=None):
        body_json = {"epoch": epoch, "messages": msgs, "prev_id": prev_id}
        res = self._call("POST", self._url(node, tid, partition), json=body_json)
        if res is None:
            return False
        status, body = res
        if status == 200:
            return True
        if status == 409 and body.get("error") in ("gap", "mismatch"):
            sources, _ = self._ranked(tid, partition, [r for r in replicas if r != node])
            if self._sync_replica(node, tid, partition, msgs[0]["offset"], sources, epoch):
                res = self._call("POST", self._url(node, tid, partition), json=body_json)
                if res is not None and res[0] == 409 and res[1].get("error") == "gap":
                    # The other replicas already expired the missing range.
                    res = self._call("POST", self._url(node, tid, partition),
                                     json={"epoch": epoch, "messages": msgs, "reset": True})
                return res is not None and res[0] == 200
        return False

    def _fetch_range(self, tid, partition, start, stop, sources):
        """Messages [start, stop) from the first source that has them all."""
        for src in sources:
            out, off = [], start
            ok = True
            while off < stop:
                res = self._call("GET", self._url(src, tid, partition),
                                 params={"offset": off, "limit": FETCH_BATCH})
                if res is None or res[0] != 200 or res[1]["end"] < stop:
                    ok = False
                    break
                msgs = [m for m in res[1]["messages"] if m["offset"] < stop]
                if not msgs:
                    break
                out.extend(msgs)
                off = msgs[-1]["offset"] + 1
            if ok:
                return out
        return None

    def _sync_replica(self, node, tid, partition, upto, sources, epoch):
        """Bring `node`'s log up to offset `upto`, copying from `sources`."""
        info = self._call("GET", self._url(node, tid, partition, "/offset"))
        if info is None or info[0] != 200:
            return False
        start, end = info[1]["start"], info[1]["end"]
        check_from = max(start, end - SYNC_WINDOW)
        theirs = self._fetch_range(tid, partition, check_from, upto, sources)
        if theirs is None:
            return False
        # Find where this replica diverges (an uncommitted write from an old leader).
        mine = self._call("GET", self._url(node, tid, partition),
                          params={"offset": check_from, "limit": end - check_from})
        resend_from = end
        if mine is not None and mine[0] == 200:
            by_offset = {m["offset"]: m for m in theirs}
            for m in mine[1]["messages"]:
                t = by_offset.get(m["offset"])
                if t is not None and t["id"] != m["id"]:
                    resend_from = m["offset"]
                    break
        to_send = [m for m in theirs if m["offset"] >= resend_from]
        if not to_send:
            return True
        reset = to_send[0]["offset"] > end  # source already expired what this replica lacks
        prev = {m["offset"]: m["id"] for m in theirs}.get(to_send[0]["offset"] - 1)
        for i in range(0, len(to_send), FETCH_BATCH):
            chunk = to_send[i:i + FETCH_BATCH]
            res = self._call("POST", self._url(node, tid, partition),
                             json={"epoch": epoch, "messages": chunk, "reset": reset and i == 0,
                                   "prev_id": prev})
            prev = chunk[-1]["id"]
            if res is None or res[0] != 200:
                return False
        log.info("Caught up replica %s for %s/%d (%d messages)", node, tid, partition, len(to_send))
        return True

    # ----- reads -----

    def read(self, topic, partition, offset, limit, epoch):
        """
        Read committed messages: from all replicas in parallel, using the most up-to-date
        one, and never past the committed end (Kafka's high watermark), so consumers do
        not see a message that may still be replaced.
        """
        tid = topic["id"]
        replicas = self.replicas(topic, partition)
        with self._lock((tid, partition)):
            committed = self._state(tid, partition, replicas, quorum(len(replicas)), epoch)[1]
        limit = min(limit, committed - offset)
        if limit <= 0:
            return []
        results = self._parallel(
            lambda n: self._call("GET", self._url(n, tid, partition),
                                 params={"offset": offset, "limit": limit}),
            self._healthy(self.replicas(topic, partition), 1))
        best = None
        for res in results.values():
            if res is not None and res[0] == 200 and (
                    best is None or (res[1]["last_gen"], res[1]["end"]) > (best["last_gen"], best["end"])):
                best = res[1]
        if best is None:
            raise QuorumError(f"No replica of partition {partition} is reachable")
        return [m for m in best["messages"] if m["offset"] < committed]

    # ----- maintenance (run periodically by the leader) -----

    def enforce_retention(self, topic, partition):
        if topic["retention_seconds"] <= 0:
            return
        before = time.time() - topic["retention_seconds"]
        for n in self.replicas(topic, partition):
            self._call("POST", self._url(n, topic["id"], partition, "/expire"), json={"before": before})

    def repair(self, topic, partition, epoch):
        tid = topic["id"]
        replicas = self.replicas(topic, partition)
        # No partition lock: storage writes are idempotent and gap-checked, so repair can
        # run alongside publishes, and a slow replica here never blocks them.
        ranked, info = self._ranked(tid, partition, self._healthy(replicas, 2))
        if len(ranked) < 2:
            return
        best = info[ranked[0]]
        for node in ranked[1:]:
            if (info[node]["end"], info[node]["last_id"]) != (best["end"], best["last_id"]):
                self._sync_replica(node, tid, partition, best["end"],
                                   [s for s in ranked if s != node], epoch)

    def collect_garbage(self, live_topic_ids):
        """
        Delete data for topics that no longer exist (e.g. a node was down during
        the delete). Storage is listed before metadata is read, so a topic created
        in between is never mistaken for garbage.
        """
        stored = {}
        for node in self.nodes:
            res = self._call("GET", f"{node}/topics")
            if res is not None and res[0] == 200:
                stored[node] = res[1]["topic_ids"]
        live = live_topic_ids()
        for node, tids in stored.items():
            for tid in tids:
                if tid not in live:
                    self._call("DELETE", f"{node}/store/{tid}")

    def delete_topic_data(self, topic_id):
        self._parallel(lambda n: self._call("DELETE", f"{n}/store/{topic_id}"), self.nodes)
        with self._locks_lock:
            for key in [k for k in self.next_offset if k[0] == topic_id]:
                del self.next_offset[key]
