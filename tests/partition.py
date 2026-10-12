"""
Network-partition tests.

Every node-to-node connection (broker -> broker for Raft and forwarding,
broker -> storage for reads and writes) goes through an in-process TCP proxy,
so the test can cut and heal individual links while every process stays up.
A cut link behaves like a lost network: existing connections are reset and new
ones hang until the caller times out. Clients still reach every node directly,
which is the worst case: a client can keep writing to a cut-off leader.

    python tests/partition.py      # from the project root, with requirements installed

Checks, for each scenario, that every publish acknowledged by any broker is
stored exactly once at the offset it was acknowledged with, that all replicas
end up identical, and that the cluster settles on a single leader after healing.
"""
import asyncio
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback

import requests

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
WORK = tempfile.mkdtemp(prefix="pubsub-partition-")
BROKERS = {f"b{i}": 9300 + 10 * (i - 1) for i in (1, 2, 3)}
STORES = {f"s{i}": 9301 + 10 * (i - 1) for i in (1, 2, 3)}
PROXY_BASE = 9400
procs = {}
results = []


# ---------------- chaos proxy ----------------

class ChaosNet:
    """TCP proxies, one per directed link (src, dst), that can be cut and healed."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.links = {}    # (src, dst) -> proxy port
        self.blocked = set()
        self.conns = {}    # (src, dst) -> set of open writers
        threading.Thread(target=self.loop.run_forever, daemon=True).start()

    def add(self, src, dst, target_port):
        port = PROXY_BASE + len(self.links)
        self.links[(src, dst)] = port
        asyncio.run_coroutine_threadsafe(self._serve((src, dst), port, target_port), self.loop).result()
        return port

    async def _serve(self, link, port, target):
        async def handle(reader, writer):
            if link in self.blocked:
                await self._blackhole(reader, writer)
                return
            try:
                r2, w2 = await asyncio.open_connection("127.0.0.1", target)
            except OSError:
                writer.close()
                return
            self.conns.setdefault(link, set()).update({writer, w2})
            await asyncio.gather(self._pipe(reader, w2), self._pipe(r2, writer))
            self.conns[link].difference_update({writer, w2})
        await asyncio.start_server(handle, "127.0.0.1", port)

    @staticmethod
    async def _pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    @staticmethod
    async def _blackhole(reader, writer):
        try:
            while await reader.read(65536):
                pass
        except Exception:
            pass
        writer.close()

    def _set(self, links, cut):
        def apply():
            for link in links:
                if cut:
                    self.blocked.add(link)
                    for w in list(self.conns.get(link, ())):
                        w.close()
                else:
                    self.blocked.discard(link)
        self.loop.call_soon_threadsafe(apply)
        time.sleep(0.1)

    def cut(self, group_a, group_b):
        """Cut every link between two groups of nodes, in both directions."""
        self._set([l for l in self.links if (l[0] in group_a and l[1] in group_b)
                   or (l[0] in group_b and l[1] in group_a)], True)

    def heal_all(self):
        self._set(list(self.links), False)


net = ChaosNet()


def start(name):
    env = dict(os.environ, MAINTENANCE_INTERVAL="1")
    if name.startswith("s"):
        env["STORAGE_DATA_DIR"] = os.path.join(WORK, name)
        app, port = "storage.server:app", STORES[name]
    else:
        # This broker reaches its peers and the storage nodes only through the proxies.
        env["BROKER_ID"] = name
        env["BROKER_DATA_DIR"] = os.path.join(WORK, name)
        env["BROKER_PEERS"] = ",".join(
            f"{b}=http://127.0.0.1:{p if b == name else net.links[(name, b)]}" for b, p in BROKERS.items())
        env["STORAGE_NODES"] = ",".join(f"http://127.0.0.1:{net.links[(name, s)]}" for s in STORES)
        app, port = "broker.server:app", BROKERS[name]
    log = open(os.path.join(WORK, name + ".log"), "a")
    procs[name] = subprocess.Popen(
        [PY, "-m", "uvicorn", app, "--host", "127.0.0.1", "--port", str(port)],
        cwd=PROJ, env=env, stdout=log, stderr=subprocess.STDOUT)


def url(n):
    return f"http://127.0.0.1:{BROKERS[n] if n in BROKERS else STORES[n]}"


def status(b):
    try:
        return requests.get(url(b) + "/health", timeout=1).json()
    except Exception:
        return None


def leaders():
    return [b for b in BROKERS if (s := status(b)) and s["role"] == "leader"]


def wait_for(pred, timeout):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(0.1)
    return None


def ready_leader(exclude=(), timeout=20):
    def find():
        for b in leaders():
            if b in exclude:
                continue
            try:
                if requests.get(url(b) + "/topics", timeout=2).status_code == 200:
                    return b
            except Exception:
                pass
    return wait_for(find, timeout)


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))


class Publisher(threading.Thread):
    """Publishes unique payloads in a loop to the given brokers, recording every outcome."""

    def __init__(self, tag, targets):
        super().__init__(daemon=True)
        self.tag, self.targets = tag, targets
        self.stop = threading.Event()
        self.acked, self.failed = [], 0

    def run(self):
        i = 0
        while not self.stop.is_set():
            b = self.targets[i % len(self.targets)]
            payload = f"{self.tag}-{i}"
            try:
                r = requests.post(url(b) + "/topics/p/messages",
                                  json={"payload": payload, "key": str(i % 4)}, timeout=5)
                if r.status_code == 200:
                    j = r.json()
                    self.acked.append((payload, j["partition"], j["offset"], b))
                else:
                    self.failed += 1
            except requests.RequestException:
                self.failed += 1
            i += 1
            time.sleep(0.03)


def replica_logs(topic):
    """{partition: [log on s1, s2, s3]} read directly from the storage nodes."""
    out = {}
    for p in range(topic["partitions"]):
        out[p] = [requests.get(f"{url(s)}/store/{topic['id']}/{p}", params={"limit": 100000}, timeout=5)
                  .json()["messages"] for s in STORES]
    return out


def verify(scenario, publishers):
    leader = ready_leader(timeout=20)
    topic = [t for t in requests.get(url(leader) + "/topics", timeout=5).json()["topics"] if t["name"] == "p"][0]

    def identical():
        logs = replica_logs(topic)
        return logs if all(all([m["id"] for m in l] == [m["id"] for m in ls[0]] for l in ls)
                           for ls in logs.values()) else None
    logs = wait_for(identical, 15)
    check(f"{scenario}: replicas identical after healing", logs is not None)
    if logs is None:
        logs = replica_logs(topic)
    stored = {(p, m["offset"]): m["payload"] for p, ls in logs.items() for m in ls[0]}
    acked = [a for pub in publishers for a in pub.acked]
    wrong = [a for a in acked if stored.get((a[1], a[2])) != a[0]]
    check(f"{scenario}: all {len(acked)} acknowledged messages stored at their offsets", not wrong, wrong[:5])
    payloads = [v for v in stored.values()]
    check(f"{scenario}: no message stored twice", len(payloads) == len(set(payloads)))
    one = wait_for(lambda: len(leaders()) == 1 or None, 10)
    check(f"{scenario}: single leader after healing", one)
    lengths = wait_for(lambda: (ls := {status(b)["commit_index"] for b in BROKERS}) and len(ls) == 1 and ls, 10)
    check(f"{scenario}: brokers agree on the Raft log", lengths)


def main():
    for b in BROKERS:
        for other in BROKERS:
            if other != b:
                net.add(b, other, BROKERS[other])
        for s in STORES:
            net.add(b, s, STORES[s])
    for n in list(STORES) + list(BROKERS):
        start(n)
    leader = ready_leader()
    print("leader:", leader)
    requests.post(url(leader) + "/topics", json={"name": "p", "partitions": 2}, timeout=10).raise_for_status()

    # 1. The leader is cut off from the other brokers but can still reach storage,
    #    and clients keep writing to it: the classic split-brain setup.
    old = leader
    rest = [b for b in BROKERS if b != old]
    pubs = [Publisher("old", [old]), Publisher("rest", rest)]
    for p in pubs:
        p.start()
    time.sleep(1.5)
    net.cut({old}, set(rest))
    t0 = time.time()
    new = ready_leader(exclude=(old,), timeout=15)
    check("leader cut off: majority side elects a new leader", new, )
    print(f"   new leader {new} after {time.time() - t0:.1f}s")
    stepped = wait_for(lambda: (s := status(old)) and s["role"] != "leader", 5)
    check("leader cut off: old leader steps down", stepped)
    print(f"   old leader stepped down after {time.time() - t0:.1f}s")
    before = len(pubs[1].acked)
    time.sleep(6)  # long enough for a follower's forward to the old leader to time out
    check("leader cut off: majority side keeps accepting publishes", len(pubs[1].acked) > before + 10,
          len(pubs[1].acked) - before)
    late_old = [a for a in pubs[0].acked if a[3] == old]
    net.heal_all()
    time.sleep(3)
    for p in pubs:
        p.stop.set()
        p.join()
    print(f"   acked via old leader: {len(late_old)}, via the others: {len(pubs[1].acked)}, failed: {sum(p.failed for p in pubs)}")
    s_old = wait_for(lambda: (s := status(old)) and s["role"] == "follower" and s["leader"] in BROKERS and s, 10)
    check("leader cut off: old leader rejoins as follower", s_old)
    verify("leader cut off", pubs)

    # 2. The leader cannot reach one storage node, which stays up for everyone else.
    leader = ready_leader()
    victim = "s2"
    pubs = [Publisher("st", [leader])]
    pubs[0].start()
    time.sleep(1)
    net.cut({leader}, {victim})
    before = len(pubs[0].acked)
    time.sleep(3)
    check("storage link cut: publishing continues on 2 of 3 replicas", len(pubs[0].acked) > before + 30,
          len(pubs[0].acked) - before)
    net.heal_all()
    time.sleep(1)
    pubs[0].stop.set()
    pubs[0].join()
    verify("storage link cut", pubs)

    # 3. A follower is cut off from both other brokers, keeps calling elections
    #    (raising its term), then rejoins.
    leader = ready_leader()
    follower = [b for b in BROKERS if b != leader][0]
    pubs = [Publisher("f", [b for b in BROKERS if b != follower])]
    pubs[0].start()
    net.cut({follower}, {b for b in BROKERS if b != follower})
    time.sleep(4)
    check("follower cut off: cluster keeps a leader",
          any(b != follower for b in leaders()))
    check("follower cut off: it cannot elect itself", status(follower)["role"] != "leader")
    net.heal_all()
    back = ready_leader(timeout=15)
    check("follower cut off: cluster settles after healing", back)
    time.sleep(2)
    pubs[0].stop.set()
    pubs[0].join()
    verify("follower cut off", pubs)

    # 4. Replica divergence after a failed publish (storage layer, driven directly).
    divergence_scenarios()


def divergence_scenarios():
    """
    Drive the storage layer directly (as a broker would) to recreate replica divergence:
    a failed publish leaves message X on one replica A only; the next publish reuses that
    offset for Y on the other two while A is unreachable; A then comes back.
    """
    sys.path.insert(0, PROJ)
    from broker.storage_client import StorageCluster, QuorumError
    # Stop the brokers: their garbage collection would delete these test-only topics.
    for b in BROKERS:
        procs.pop(b).kill()
    for s in STORES:
        net.add("t", s, STORES[s])
    sc = StorageCluster([f"http://127.0.0.1:{net.links[('t', s)]}" for s in STORES])
    epoch = 10_000  # newer than any epoch the brokers used on their own topics

    def run(name, heal_with):
        topic = {"id": f"div-{name}", "replica_factor": 3, "partitions": 1}
        reps = sc.replicas(topic, 0)
        node = {u: s for s in STORES for u in [f"http://127.0.0.1:{net.links[('t', s)]}"]}
        a, others = node[reps[0]], {node[r] for r in reps[1:]}
        msg = lambda p: {"id": p, "topic": topic["id"], "partition": 0, "payload": p, "timestamp": time.time(), "key": None}
        sc.publish(topic, 0, msg("m0"), epoch)
        sc.publish(topic, 0, msg("m1"), epoch)
        net.cut({"t"}, others)  # only A reachable: X lands on A alone
        try:
            sc.publish(topic, 0, msg("X"), epoch)
            check(f"divergence ({name}): publish without quorum fails", False)
        except QuorumError:
            check(f"divergence ({name}): publish without quorum fails", True)
        seen = [m["id"] for m in sc.read(topic, 0, 0, 100, epoch)]
        check(f"divergence ({name}): consumers never see the unacknowledged message", seen == ["m0", "m1"], seen)
        net.heal_all()
        net.cut({"t"}, {a})     # A unreachable: Y goes to the other two
        y = sc.publish(topic, 0, msg("Y"), epoch)
        net.heal_all()
        time.sleep(SUSPECT_WAIT)
        heal_with(topic)
        time.sleep(1)  # the write to A finishes in the background after the quorum returns
        logs = [[m["id"] for m in requests.get(f"{url(s)}/store/{topic['id']}/0", params={"limit": 100}).json()["messages"]]
                for s in STORES]
        check(f"divergence ({name}): replicas identical, acknowledged Y kept at offset {y['offset']}",
              all(l == logs[0] for l in logs) and logs[0][y["offset"]] == "Y" and "X" not in logs[0], logs)

    run("next-write", lambda topic: sc.publish(topic, 0, {"id": "Z", "topic": topic["id"], "partition": 0,
                                                          "payload": "Z", "timestamp": time.time(), "key": None}, epoch))
    run("repair", lambda topic: sc.repair(topic, 0, epoch))


SUSPECT_WAIT = 4.5  # past the 2 s timeout plus the 2 s skip window, so A receives the next write


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        results.append(("exception", False))
    finally:
        for n in list(procs):
            procs[n].send_signal(signal.SIGKILL)
            procs[n].wait()
        if "--keep" in sys.argv:
            print("data and logs kept in", WORK)
        else:
            shutil.rmtree(WORK, ignore_errors=True)
    passed = sum(ok for _, ok in results)
    print(f"\n{passed}/{len(results)} passed")
    sys.exit(0 if passed == len(results) else 1)
