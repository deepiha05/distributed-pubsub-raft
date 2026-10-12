# broker/raft.py
"""
Raft consensus (Ongaro & Ousterhout, 2014) for the broker cluster.

The brokers replicate a log of metadata commands (create topic, create
subscription, commit offsets, ...). A command takes effect once a majority of
brokers have stored it; every broker then applies it, in log order, to its
MetadataStore. If the leader fails, the remaining majority elects a new one.

Implemented: leader election with randomized timeouts, log replication with
consistency checks, majority commit, persistence of term/vote/log to disk, and
CheckQuorum (a leader that loses contact with a majority steps down).
Not implemented: log compaction (snapshots) and membership changes.
"""
import json
import logging
import os
import random
import threading
import time

import requests

log = logging.getLogger("broker.raft")

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"
MAX_BATCH = 100


class NotLeaderError(Exception):
    def __init__(self, leader_id=None):
        super().__init__(f"Not the leader (leader is {leader_id})")
        self.leader_id = leader_id


class RaftNode:
    def __init__(self, node_id, peers, data_dir, apply_fn,
                 election_timeout=(1.0, 2.0), heartbeat=0.2, rpc_timeout=1.0, fsync=True):
        self.id = node_id
        self.urls = dict(peers)
        self.peers = [pid for pid in peers if pid != node_id]
        self.majority = (len(self.peers) + 1) // 2 + 1
        self.apply_fn = apply_fn
        self.election_timeout = election_timeout
        self.heartbeat = heartbeat
        self.rpc_timeout = rpc_timeout
        self.fsync = fsync

        os.makedirs(data_dir, exist_ok=True)
        self.state_path = os.path.join(data_dir, "raft_state.json")
        self.log_path = os.path.join(data_dir, "raft_log.jsonl")

        self.cond = threading.Condition()  # guards all fields below
        # Persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = [{"term": 0, "command": None}]  # index 0 is a sentinel
        self._load()
        # Volatile state
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.votes = set()
        self.noop_index = 0
        self.last_contact = {pid: 0.0 for pid in self.peers}  # leader: last reply from each peer
        self.waiting = set()
        self.results = {}
        self.events = {pid: threading.Event() for pid in self.peers}
        self.stopped = threading.Event()
        self._reset_election_timer()

    # ----- persistence -----

    def _write(self, f):
        f.flush()
        if self.fsync:
            os.fsync(f.fileno())

    def _load(self):
        if os.path.exists(self.state_path):
            with open(self.state_path) as f:
                state = json.load(f)
            self.current_term = state["current_term"]
            self.voted_for = state["voted_for"]
        if os.path.exists(self.log_path):
            with open(self.log_path) as f:
                for line in f:
                    if not line.endswith("\n"):
                        break  # torn write from a crash
                    self.log.append(json.loads(line))
            self._rewrite_log()

    def _persist_state(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"current_term": self.current_term, "voted_for": self.voted_for}, f)
            self._write(f)
        os.replace(tmp, self.state_path)

    def _append_to_disk(self, entries):
        with open(self.log_path, "a") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
            self._write(f)

    def _rewrite_log(self):
        tmp = self.log_path + ".tmp"
        with open(tmp, "w") as f:
            for e in self.log[1:]:
                f.write(json.dumps(e) + "\n")
            self._write(f)
        os.replace(tmp, self.log_path)

    # ----- helpers (call with self.cond held) -----

    @property
    def last_index(self):
        return len(self.log) - 1

    def _reset_election_timer(self):
        self.election_deadline = time.monotonic() + random.uniform(*self.election_timeout)

    def _step_down(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.leader_id = None
            self._persist_state()
        if self.role != FOLLOWER:
            log.info("%s: stepping down to follower in term %d", self.id, self.current_term)
        self.role = FOLLOWER
        self._reset_election_timer()
        self.cond.notify_all()

    def _append_local(self, command):
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        self._append_to_disk([entry])
        return self.last_index

    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.log[n]["term"] != self.current_term:
                break  # Raft only commits entries from the current term by counting
            replicas = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if replicas >= self.majority:
                self.commit_index = n
                self.cond.notify_all()
                break

    # ----- elections -----

    def _start_election(self):
        self.role = CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.leader_id = None
        self._persist_state()
        self._reset_election_timer()
        self.votes = {self.id}
        log.info("%s: starting election for term %d", self.id, self.current_term)
        if len(self.votes) >= self.majority:
            self._become_leader()
            return
        args = {"term": self.current_term, "candidate_id": self.id,
                "last_log_index": self.last_index, "last_log_term": self.log[-1]["term"]}
        for pid in self.peers:
            threading.Thread(target=self._request_vote, args=(pid, args), daemon=True).start()

    def _request_vote(self, pid, args):
        try:
            r = requests.post(f"{self.urls[pid]}/raft/request_vote", json=args,
                              timeout=self.rpc_timeout).json()
        except Exception:
            return
        with self.cond:
            if r["term"] > self.current_term:
                self._step_down(r["term"])
                return
            if self.role != CANDIDATE or self.current_term != args["term"]:
                return
            if r["vote_granted"]:
                self.votes.add(pid)
                if len(self.votes) >= self.majority:
                    self._become_leader()

    def _become_leader(self):
        log.info("%s: became leader for term %d", self.id, self.current_term)
        self.role = LEADER
        self.leader_id = self.id
        now = time.monotonic()
        for pid in self.peers:
            self.next_index[pid] = self.last_index + 1
            self.match_index[pid] = 0
            self.last_contact[pid] = now
        # A no-op from the new term lets the leader commit entries left by earlier terms.
        self.noop_index = self._append_local({"op": "noop"})
        self._advance_commit()
        for ev in self.events.values():
            ev.set()

    # ----- background threads -----

    def start(self):
        threading.Thread(target=self._ticker, daemon=True).start()
        threading.Thread(target=self._applier, daemon=True).start()
        for pid in self.peers:
            threading.Thread(target=self._replicator, args=(pid,), daemon=True).start()

    def stop(self):
        self.stopped.set()
        with self.cond:
            self.cond.notify_all()

    def _ticker(self):
        while not self.stopped.wait(0.05):
            with self.cond:
                if self.role != LEADER and time.monotonic() >= self.election_deadline:
                    self._start_election()
                elif self.role == LEADER and not self._has_quorum():
                    # CheckQuorum: a leader cut off from a majority steps down rather than
                    # keep serving requests it cannot commit.
                    log.info("%s: lost contact with a majority, stepping down", self.id)
                    self.leader_id = None
                    self._step_down(self.current_term)

    def _has_quorum(self):
        now = time.monotonic()
        reachable = 1 + sum(1 for t in self.last_contact.values() if now - t < self.election_timeout[1])
        return reachable >= self.majority

    def _replicator(self, pid):
        ev = self.events[pid]
        session = requests.Session()
        url = f"{self.urls[pid]}/raft/append_entries"
        while not self.stopped.is_set():
            ev.wait(self.heartbeat)
            ev.clear()
            with self.cond:
                if self.role != LEADER:
                    continue
                term = self.current_term
                ni = self.next_index[pid]
                prev = ni - 1
                entries = self.log[ni:ni + MAX_BATCH]
                args = {"term": term, "leader_id": self.id, "prev_log_index": prev,
                        "prev_log_term": self.log[prev]["term"], "entries": entries,
                        "leader_commit": self.commit_index}
            try:
                r = session.post(url, json=args, timeout=self.rpc_timeout).json()
            except Exception:
                continue
            with self.cond:
                if r["term"] > self.current_term:
                    self._step_down(r["term"])
                    continue
                if self.role != LEADER or self.current_term != term:
                    continue
                self.last_contact[pid] = time.monotonic()
                if r["success"]:
                    match = prev + len(entries)
                    if match > self.match_index[pid]:
                        self.match_index[pid] = match
                        self._advance_commit()
                    self.next_index[pid] = max(self.next_index[pid], match + 1)
                    if self.next_index[pid] <= self.last_index:
                        ev.set()
                else:
                    self.next_index[pid] = max(1, min(ni - 1, r.get("last_index", ni - 1) + 1))
                    ev.set()

    def _applier(self):
        while not self.stopped.is_set():
            with self.cond:
                while self.last_applied >= self.commit_index and not self.stopped.is_set():
                    self.cond.wait(0.5)
                if self.stopped.is_set():
                    return
                idx = self.last_applied + 1
                command = self.log[idx]["command"]
            try:
                result = (True, self.apply_fn(command))
            except Exception as e:
                result = (False, e)
            with self.cond:
                self.last_applied = idx
                if idx in self.waiting:
                    self.results[idx] = result
                self.cond.notify_all()

    # ----- RPC handlers -----

    def handle_request_vote(self, args):
        with self.cond:
            if args["term"] > self.current_term:
                self._step_down(args["term"])
            my_last = (self.log[-1]["term"], self.last_index)
            their_last = (args["last_log_term"], args["last_log_index"])
            granted = (args["term"] == self.current_term
                       and self.voted_for in (None, args["candidate_id"])
                       and their_last >= my_last)
            if granted:
                self.voted_for = args["candidate_id"]
                self._persist_state()
                self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": granted}

    def handle_append_entries(self, args):
        with self.cond:
            if args["term"] < self.current_term:
                return {"term": self.current_term, "success": False, "last_index": self.last_index}
            if args["term"] > self.current_term or self.role != FOLLOWER:
                self._step_down(args["term"])
            self.leader_id = args["leader_id"]
            self._reset_election_timer()

            prev = args["prev_log_index"]
            if prev > self.last_index:
                return {"term": self.current_term, "success": False, "last_index": self.last_index}
            if self.log[prev]["term"] != args["prev_log_term"]:
                return {"term": self.current_term, "success": False, "last_index": prev - 1}

            truncated, new = False, []
            for i, entry in enumerate(args["entries"]):
                idx = prev + 1 + i
                if idx <= self.last_index:
                    if self.log[idx]["term"] == entry["term"]:
                        continue
                    del self.log[idx:]
                    truncated = True
                self.log.append(entry)
                new.append(entry)
            if truncated:
                self._rewrite_log()
            elif new:
                self._append_to_disk(new)

            if args["leader_commit"] > self.commit_index:
                self.commit_index = max(self.commit_index,
                                        min(args["leader_commit"], prev + len(args["entries"])))
                self.cond.notify_all()
            return {"term": self.current_term, "success": True, "last_index": self.last_index}

    # ----- client API -----

    def propose(self, command, timeout=5.0):
        """Append a command, wait until a majority commits and it is applied, return its result."""
        with self.cond:
            if self.role != LEADER:
                raise NotLeaderError(self.leader_id)
            term = self.current_term
            idx = self._append_local(command)
            self.waiting.add(idx)
            self._advance_commit()
        for ev in self.events.values():
            ev.set()
        deadline = time.monotonic() + timeout
        with self.cond:
            try:
                while self.last_applied < idx:
                    if self.role != LEADER or self.current_term != term:
                        raise NotLeaderError(self.leader_id)
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Timed out waiting for a majority of brokers")
                    self.cond.wait(remaining)
                ok, value = self.results.pop(idx)
            finally:
                self.waiting.discard(idx)
                self.results.pop(idx, None)
        if not ok:
            raise value
        return value

    def is_leader(self):
        with self.cond:
            return self.role == LEADER

    def _caught_up(self):
        # A new leader has applied every committed entry once its own no-op is applied.
        return self.last_applied >= self.noop_index

    def leader_ready(self):
        """True once this leader's state machine reflects every committed command."""
        with self.cond:
            return self.role == LEADER and self._caught_up()

    def leader_epoch(self):
        """The current term, if this node is a caught-up leader. Used to fence storage writes."""
        with self.cond:
            if self.role != LEADER or not self._caught_up():
                raise NotLeaderError(self.leader_id)
            return self.current_term

    def leader_url(self):
        with self.cond:
            return self.urls.get(self.leader_id) if self.leader_id else None

    def status(self):
        with self.cond:
            return {"id": self.id, "role": self.role, "term": self.current_term,
                    "leader": self.leader_id, "commit_index": self.commit_index,
                    "last_applied": self.last_applied, "log_length": self.last_index,
                    "peers": self.urls}
