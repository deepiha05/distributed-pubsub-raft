"""
End-to-end fault-injection test.

Starts 3 storage nodes and 3 brokers on localhost (ports 9000-9021), drives the
REST API like a client, and kills processes with SIGKILL to simulate crashes.

    python tests/e2e.py            # from the project root, with requirements installed

Data and logs go to a temporary directory that is removed afterwards
(pass --keep to keep it).
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import requests

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
WORK = tempfile.mkdtemp(prefix="pubsub-e2e-")
DATA = os.path.join(WORK, "data")
LOGS = os.path.join(WORK, "logs")
BROKERS = {f"b{i}": 9000 + 10 * (i - 1) for i in (1, 2, 3)}
STORES = {f"s{i}": 9001 + 10 * (i - 1) for i in (1, 2, 3)}
ENV = {
    "STORAGE_NODES": ",".join(f"http://127.0.0.1:{p}" for p in STORES.values()),
    "BROKER_PEERS": ",".join(f"{b}=http://127.0.0.1:{p}" for b, p in BROKERS.items()),
    "MAINTENANCE_INTERVAL": "1",
}
procs = {}
results = []


def start(name):
    env = dict(os.environ, **ENV)
    if name.startswith("s"):
        env["STORAGE_DATA_DIR"] = os.path.join(DATA, name)
        app, port = "storage.server:app", STORES[name]
    else:
        env["BROKER_ID"] = name
        env["BROKER_DATA_DIR"] = os.path.join(DATA, name)
        app, port = "broker.server:app", BROKERS[name]
    log = open(os.path.join(LOGS, name + ".log"), "a")
    procs[name] = subprocess.Popen(
        [PY, "-m", "uvicorn", app, "--host", "127.0.0.1", "--port", str(port)],
        cwd=PROJ, env=env, stdout=log, stderr=subprocess.STDOUT)


def kill(name):
    p = procs.pop(name)
    p.send_signal(signal.SIGKILL)
    p.wait()


def url(b):
    return f"http://127.0.0.1:{BROKERS[b]}"


def status(b):
    try:
        return requests.get(url(b) + "/health", timeout=1).json()
    except Exception:
        return None


def wait_leader(exclude=(), timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        for b in BROKERS:
            if b in exclude or b not in procs:
                continue
            s = status(b)
            if s and s["role"] == "leader":
                try:
                    if requests.get(url(b) + "/topics", timeout=2).status_code == 200:
                        return b
                except Exception:
                    pass
        time.sleep(0.2)
    raise AssertionError("no leader elected")


def req(method, b, path, ok=True, **kw):
    r = requests.request(method, url(b) + path, timeout=15, **kw)
    if ok and r.status_code != 200:
        raise AssertionError(f"{method} {path} via {b} -> {r.status_code} {r.text}")
    return r


def drain(b, sid, consumer=None, ack=True):
    """Pull (and ack) until a pull comes back empty."""
    out = []
    while True:
        params = {"limit": 50}
        if consumer:
            params["consumer_id"] = consumer
        msgs = req("GET", b, f"/subscriptions/{sid}/messages", params=params).json()["messages"]
        if not msgs:
            return out
        out.extend(msgs)
        if not ack:
            return out
        acks = {}
        for m in msgs:
            acks[m["partition"]] = max(acks.get(m["partition"], -1), m["offset"])
        req("POST", b, f"/subscriptions/{sid}/ack",
            json={"acks": [{"partition": p, "offset": o} for p, o in acks.items()]})


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))


def main():
    os.makedirs(LOGS)
    for n in list(STORES) + list(BROKERS):
        start(n)
    leader = wait_leader()
    followers = [b for b in BROKERS if b != leader]
    print("leader:", leader)

    # 1. Basic flow through followers (forwarding), no stall
    t0 = time.time()
    req("POST", followers[0], "/topics", json={"name": "orders", "partitions": 3, "retention_seconds": 0})
    check("create topic via follower is fast", time.time() - t0 < 1.5, f"{time.time() - t0:.2f}s")
    r = req("POST", followers[1], "/topics", ok=False,
            json={"name": "orders", "partitions": 3, "retention_seconds": 0})
    check("duplicate topic rejected", r.status_code == 400, r.text)
    time.sleep(0.5)
    check("topic visible on all brokers",
          any(t["name"] == "orders" for t in req("GET", followers[1], "/topics").json()["topics"]))

    sid = req("POST", followers[0], "/subscriptions",
              json={"subscriber_id": "c1", "topic": "orders"}).json()["subscription"]["id"]
    t0 = time.time()
    for i in range(30):
        req("POST", followers[i % 2], "/topics/orders/messages", json={"payload": f"m{i}", "key": f"k{i % 3}"})
    check("30 publishes via followers < 6s", time.time() - t0 < 6, f"{time.time() - t0:.2f}s")
    first = req("GET", leader, f"/subscriptions/{sid}/messages", params={"limit": 100}).json()["messages"]
    check("pull limit is per pull",
          len(req("GET", leader, f"/subscriptions/{sid}/messages", params={"limit": 7}).json()["messages"]) == 7)
    again = req("GET", leader, f"/subscriptions/{sid}/messages", params={"limit": 100}).json()["messages"]
    check("unacked messages redelivered with attempts counted",
          len(again) == 30 and all(m["delivery_attempts"] >= 2 for m in again))
    by_key = {}
    for m in first:
        by_key.setdefault(m["key"], set()).add(m["partition"])
    check("same key -> same partition", all(len(v) == 1 for v in by_key.values()))
    ordered = True
    for k in by_key:
        seq = [int(m["payload"][1:]) for m in sorted(first, key=lambda m: m["offset"]) if m["key"] == k]
        ordered &= seq == sorted(seq)
    check("per-key order", ordered)
    check("drain returns all 30", len(drain(followers[1], sid)) == 30)
    check("after ack, pull is empty",
          req("GET", followers[0], f"/subscriptions/{sid}/messages").json()["messages"] == [])

    # Replica consistency: same offset -> same message on every replica
    topic = [t for t in req("GET", leader, "/topics").json()["topics"] if t["name"] == "orders"][0]
    consistent = True
    for p in range(3):
        logs = []
        for sp in STORES.values():
            d = requests.get(f"http://127.0.0.1:{sp}/store/{topic['id']}/{p}", params={"limit": 1000}).json()
            if d["messages"]:
                logs.append([m["id"] for m in d["messages"]])
        consistent &= len(logs) in (0, 3) and all(l == logs[0] for l in logs)
    check("all 3 replicas identical per partition", consistent)

    # 2. Seek / replay
    req("POST", leader, f"/subscriptions/{sid}/seek", json={"partition": 0, "offset": 0})
    replay = req("GET", leader, f"/subscriptions/{sid}/messages", params={"limit": 100}).json()["messages"]
    check("seek replays partition 0", len(replay) > 0 and all(m["partition"] == 0 for m in replay))
    drain(leader, sid)

    # 3. One storage node down: quorum 2/3 still works; it catches up after restart
    kill("s2")
    try:
        for i in range(10):
            req("POST", leader, "/topics/orders/messages", json={"payload": f"x{i}", "key": f"k{i % 3}"})
        check("publish works with 1 of 3 storage nodes down", True)
    except AssertionError as e:
        check("publish works with 1 of 3 storage nodes down", False, str(e))
    check("consumer gets them", len(drain(leader, sid)) == 10)
    start("s2")
    time.sleep(4)  # maintenance repair runs every 1s here
    ends = [[requests.get(f"http://127.0.0.1:{sp}/store/{topic['id']}/{p}/offset").json()["end"]
             for sp in STORES.values()] for p in range(3)]
    check("restarted storage node caught up by repair", all(len(set(e)) == 1 for e in ends), ends)

    # 4. Two storage nodes down -> writes refused, nothing acknowledged is lost
    kill("s1")
    kill("s3")
    r = req("POST", leader, "/topics/orders/messages", ok=False, json={"payload": "nope"})
    check("publish refused without storage quorum", r.status_code == 503, r.text)
    start("s1")
    start("s3")
    time.sleep(1.5)

    # 5. Leader broker dies -> new leader, state preserved, writes continue
    old = leader
    kill(old)
    t0 = time.time()
    leader = wait_leader(exclude=(old,))
    check(f"new leader elected ({leader})", leader != old)
    print(f"   failover took {time.time() - t0:.1f}s")
    s = req("GET", leader, f"/subscriptions/{sid}").json()
    check("subscription & offsets survived failover", sum(s["offsets"].values()) == 40, s["offsets"])
    req("POST", leader, "/topics/orders/messages", json={"payload": "after-failover", "key": "k0"})
    got = drain(leader, sid)
    # 'nope' got a 503 but may have reached one replica; at-least-once allows delivering it.
    check("publish/pull after failover (no redelivery of acked)",
          [m["payload"] for m in got if m["payload"] != "nope"] == ["after-failover"],
          [m["payload"] for m in got])
    start(old)
    time.sleep(3)
    s_old = status(old)
    check("old leader rejoins as follower",
          s_old and s_old["role"] == "follower" and s_old["leader"] == leader, s_old)

    # 6. Majority of brokers down -> no metadata writes
    others = [b for b in BROKERS if b != leader]
    kill(others[0])
    kill(others[1])
    time.sleep(3)
    r = req("POST", leader, "/topics", ok=False, json={"name": "x", "partitions": 1})
    check("no metadata writes without broker majority", r.status_code == 503, r.text)
    r = req("POST", leader, "/topics/orders/messages", ok=False, json={"payload": "minority"})
    check("isolated leader steps down; publishes refused", r.status_code == 503 and status(leader)["role"] != "leader", r.text)
    start(others[0])
    start(others[1])
    leader = wait_leader()

    # 7. Full cluster restart -> durability
    for n in list(procs):
        kill(n)
    for n in list(STORES) + list(BROKERS):
        start(n)
    leader = wait_leader()
    followers = [b for b in BROKERS if b != leader]
    s = req("GET", leader, f"/subscriptions/{sid}").json()
    check("metadata survives full restart", s["topic"] == "orders")
    for p in range(3):
        req("POST", leader, f"/subscriptions/{sid}/seek", json={"partition": p, "offset": 0})
    all_msgs = drain(leader, sid)
    check("all 41 messages survive full restart",
          len([m for m in all_msgs if m["payload"] != "nope"]) == 41, len(all_msgs))

    # 8. Delete + recreate: no resurrection, followers agree
    req("DELETE", leader, "/topics/orders")
    time.sleep(0.5)
    check("deleted topic gone on all brokers", all(
        not any(t["name"] == "orders" for t in req("GET", b, "/topics").json()["topics"]) for b in BROKERS))
    req("POST", leader, "/topics", json={"name": "orders", "partitions": 3})
    sid2 = req("POST", leader, "/subscriptions",
               json={"subscriber_id": "c2", "topic": "orders"}).json()["subscription"]["id"]
    check("recreated topic has no old messages", drain(leader, sid2) == [])

    # 9. Retention
    req("POST", leader, "/topics", json={"name": "short", "partitions": 1, "retention_seconds": 2})
    sid3 = req("POST", leader, "/subscriptions",
               json={"subscriber_id": "c3", "topic": "short"}).json()["subscription"]["id"]
    for i in range(5):
        req("POST", leader, "/topics/short/messages", json={"payload": f"r{i}"})
    check("messages present before retention", len(drain(leader, sid3, ack=False)) == 5)
    time.sleep(4.5)
    check("messages expired after retention", drain(leader, sid3, ack=False) == [])
    req("POST", leader, "/topics/short/messages", json={"payload": "fresh"})
    check("new message after retention visible", [m["payload"] for m in drain(leader, sid3)] == ["fresh"])

    # 10. Consumer groups
    req("POST", leader, "/topics", json={"name": "grp", "partitions": 4})
    gid = req("POST", leader, "/subscriptions",
              json={"subscriber_id": "g", "topic": "grp"}).json()["subscription"]["id"]
    req("POST", leader, f"/subscriptions/{gid}/members", json={"consumer_id": "A"})
    req("POST", leader, f"/subscriptions/{gid}/members", json={"consumer_id": "B"})
    for i in range(40):
        req("POST", leader, "/topics/grp/messages", json={"payload": f"g{i}", "key": str(i)})
    ga = drain(leader, gid, consumer="A")
    gb = drain(leader, gid, consumer="B")
    pa, pb = {m["partition"] for m in ga}, {m["partition"] for m in gb}
    check("group members split partitions, no overlap, all delivered",
          not (pa & pb) and len(ga) + len(gb) == 40, (pa, pb, len(ga), len(gb)))
    r = req("GET", leader, f"/subscriptions/{gid}/messages", ok=False)
    check("group requires consumer_id", r.status_code == 400)
    req("DELETE", leader, f"/subscriptions/{gid}/members/B")
    req("POST", leader, "/topics/grp/messages", json={"payload": "solo", "key": "1"})
    check("after B leaves, A gets everything",
          [m["payload"] for m in drain(leader, gid, consumer="A")] == ["solo"])

    # 11. Dead-letter queues
    req("POST", leader, "/topics", json={"name": "jobs", "partitions": 2})
    req("POST", leader, "/topics", json={"name": "jobs-dlq", "partitions": 1})
    r = req("POST", leader, "/subscriptions", ok=False,
            json={"subscriber_id": "w", "topic": "jobs", "dead_letter_topic": "missing"})
    check("DLQ must be an existing topic", r.status_code == 404, r.text)
    r = req("POST", leader, "/subscriptions", ok=False,
            json={"subscriber_id": "w", "topic": "jobs", "dead_letter_topic": "jobs"})
    check("DLQ must differ from subscribed topic", r.status_code == 400, r.text)
    wid = req("POST", followers[0], "/subscriptions",
              json={"subscriber_id": "w", "topic": "jobs", "dead_letter_topic": "jobs-dlq",
                    "max_delivery_attempts": 3}).json()["subscription"]["id"]
    did = req("POST", leader, "/subscriptions",
              json={"subscriber_id": "ops", "topic": "jobs-dlq"}).json()["subscription"]["id"]
    for i in range(3):
        req("POST", leader, "/topics/jobs/messages", json={"payload": f"job{i}", "key": "same"})
    seen = []
    for _ in range(3):  # consumer keeps failing: pulls 2 at a time, never acks
        r = req("GET", followers[0], f"/subscriptions/{wid}/messages", params={"limit": 2}).json()
        seen.append([(m["payload"], m["delivery_attempts"]) for m in r["messages"]])
    check("delivered up to max attempts", seen[-1] == [("job0", 3), ("job1", 3)], seen)
    r = req("GET", followers[0], f"/subscriptions/{wid}/messages", params={"limit": 2}).json()
    check("next pull dead-letters job0 + job1 and delivers job2",
          r["dead_lettered"] == 2 and [m["payload"] for m in r["messages"]] == ["job2"], r)
    m = r["messages"][0]
    req("POST", leader, f"/subscriptions/{wid}/ack", json={"acks": [{"partition": m["partition"], "offset": m["offset"]}]})
    check("acked message is not dead-lettered; subscription empty",
          req("GET", leader, f"/subscriptions/{wid}/messages").json() ==
          {"messages": [], "unavailable_partitions": [], "dead_lettered": 0})
    dl = drain(leader, did)
    a = dl[0].get("attributes") or {} if dl else {}
    check("DLQ holds the 2 failed messages with their source",
          [m["payload"] for m in dl] == ["job0", "job1"] and a.get("source_topic") == "jobs"
          and a.get("source_subscription") == wid and a.get("delivery_attempts") == 3, dl)
    r = req("PUT", leader, f"/subscriptions/{wid}/dead-letter",
            json={"dead_letter_topic": "jobs-dlq", "max_delivery_attempts": 1}).json()
    check("policy can be changed", r["max_delivery_attempts"] == 1, r)
    req("DELETE", leader, "/topics/jobs-dlq")
    s = req("GET", leader, f"/subscriptions/{wid}").json()
    check("deleting the DLQ topic clears the policy",
          s["dead_letter_topic"] is None and s["max_delivery_attempts"] == 0, s)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        results.append(("exception", False))
    finally:
        for n in list(procs):
            kill(n)
        if "--keep" in sys.argv:
            print("data and logs kept in", WORK)
        else:
            shutil.rmtree(WORK, ignore_errors=True)
    passed = sum(ok for _, ok in results)
    print(f"\n{passed}/{len(results)} passed")
    sys.exit(0 if passed == len(results) else 1)
