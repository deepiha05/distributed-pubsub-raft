# storage/storage.py
"""
Durable, append-only partition logs.

Each topic partition is stored on disk as:

    <STORAGE_DATA_DIR>/<topic_id>/<partition>/log.jsonl   one JSON message per line
    <STORAGE_DATA_DIR>/<topic_id>/<partition>/meta.json   {"start_offset", "epoch"}

Offsets are assigned by the leader broker, not by the storage node, so every
replica stores the same message at the same offset. Writes carry the leader's
epoch (its Raft term); a replica rejects writes from an older epoch, which
fences off a deposed leader.

As in Raft's log matching, each write names the id of the message that must
precede it (prev_id), and a replica whose history differs rejects it
("mismatch"); every message also records the generation (`gen`) of the leader
that wrote it, so when replicas disagree the newer generation's history wins.
"""
import json
import os
import re
import shutil
import threading

FSYNC = os.environ.get("STORAGE_FSYNC", "true").lower() == "true"
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


class StorageError(Exception):
    """A replica-level rejection, returned to the broker as HTTP 409."""

    def __init__(self, kind: str, detail: str, **extra):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail
        self.extra = extra

    def to_dict(self):
        return {"error": self.kind, "detail": self.detail, **self.extra}


def _fsync_file(f):
    f.flush()
    if FSYNC:
        os.fsync(f.fileno())


def _atomic_write(path: str, data: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(data)
        _fsync_file(f)
    os.replace(tmp, path)


class PartitionLog:
    def __init__(self, path: str):
        self.path = path
        self.log_path = os.path.join(path, "log.jsonl")
        self.meta_path = os.path.join(path, "meta.json")
        self.lock = threading.Lock()
        self.messages = []   # messages[i] has offset start + i
        self.start = 0       # first offset still retained
        self.epoch = 0       # highest leader epoch seen
        self.empty_tail = {"gen": 0, "id": None}  # last message, once retention removed them all
        os.makedirs(path, exist_ok=True)
        self._load()

    @property
    def end(self) -> int:
        """Next offset to be written (log end offset)."""
        return self.start + len(self.messages)

    # ----- persistence -----

    def _load(self):
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                meta = json.load(f)
            self.start = meta.get("start_offset", 0)
            self.epoch = meta.get("epoch", 0)
            self.empty_tail = {"gen": meta.get("tail_gen", 0), "id": meta.get("tail_id")}
        if not os.path.exists(self.log_path):
            return
        good, valid_bytes = [], 0
        with open(self.log_path, "rb") as f:
            for line in f:
                if not line.endswith(b"\n"):
                    break  # torn write from a crash
                try:
                    good.append(json.loads(line))
                except ValueError:
                    break
                valid_bytes += len(line)
        if valid_bytes < os.path.getsize(self.log_path):
            with open(self.log_path, "r+b") as f:
                f.truncate(valid_bytes)
        if good:
            self.start = good[0]["offset"]
        self.messages = good

    def _write_meta(self):
        _atomic_write(self.meta_path, json.dumps({"start_offset": self.start, "epoch": self.epoch,
                                                  "tail_gen": self.empty_tail["gen"],
                                                  "tail_id": self.empty_tail["id"]}))

    def _tail(self):
        """Generation and id of the last message (needed to compare replicas)."""
        if self.messages:
            m = self.messages[-1]
            return {"last_gen": m.get("gen", 0), "last_id": m["id"]}
        return {"last_gen": self.empty_tail["gen"], "last_id": self.empty_tail["id"]}

    def _append_lines(self, msgs):
        if not msgs:
            return
        with open(self.log_path, "a") as f:
            for m in msgs:
                f.write(json.dumps(m) + "\n")
            _fsync_file(f)

    def _rewrite(self):
        _atomic_write(self.log_path, "".join(json.dumps(m) + "\n" for m in self.messages))
        self._write_meta()

    def _check_epoch(self, epoch: int):
        if epoch < self.epoch:
            raise StorageError("stale_epoch", f"Epoch {epoch} is older than {self.epoch}", epoch=self.epoch)
        if epoch > self.epoch:
            self.epoch = epoch
            self._write_meta()

    # ----- operations -----

    def append(self, epoch: int, messages: list, reset: bool = False, prev_id=None) -> int:
        """
        Store messages at the offsets the leader assigned.

        - offset < start: already expired by retention, skipped.
        - offset < end, same id: duplicate (idempotent retry), skipped.
        - offset < end, different id: this replica holds an uncommitted entry
          from an older leader; truncate it and take the leader's version.
        - offset > end: gap, rejected unless reset=True (the leader is
          re-seeding a replica that fell behind retention).
        - the message before the first one is not prev_id: this replica's
          history differs from the leader's, rejected as a mismatch.
        """
        messages = sorted(messages, key=lambda m: m["offset"])
        for a, b in zip(messages, messages[1:]):
            if b["offset"] != a["offset"] + 1:
                raise StorageError("bad_request", "Messages must have consecutive offsets")
        with self.lock:
            self._check_epoch(epoch)
            if messages and messages[0]["offset"] > self.end and not reset:
                raise StorageError("gap", f"Log ends at {self.end}", start=self.start, end=self.end)
            if messages and prev_id is not None and not reset:
                p = messages[0]["offset"] - 1
                if self.start <= p < self.end and self.messages[p - self.start]["id"] != prev_id:
                    raise StorageError("mismatch", f"Message at {p} differs from the leader's",
                                       start=self.start, end=self.end)
            rewrite, appended = False, []
            for m in messages:
                off = m["offset"]
                if off < self.start:
                    continue
                if off < self.end:
                    if self.messages[off - self.start]["id"] == m["id"]:
                        continue
                    del self.messages[off - self.start:]
                    rewrite = True
                elif off > self.end:
                    self.messages, self.start, rewrite = [], off, True
                self.messages.append(m)
                appended.append(m)
            if rewrite:
                self._rewrite()
            else:
                self._append_lines(appended)
            return self.end

    def read(self, offset: int, limit: int):
        with self.lock:
            offset = max(offset, self.start)
            i = offset - self.start
            return list(self.messages[i:i + limit]), self.start, self.end, self._tail()["last_gen"]

    def fence(self, epoch: int):
        with self.lock:
            self._check_epoch(epoch)
            return {"start": self.start, "end": self.end, **self._tail()}

    def info(self):
        with self.lock:
            return {"start": self.start, "end": self.end, "epoch": self.epoch, **self._tail()}

    def expire(self, before: float):
        """Drop messages older than `before` from the head of the log."""
        with self.lock:
            n = 0
            while n < len(self.messages) and self.messages[n]["timestamp"] < before:
                n += 1
            if n:
                if n == len(self.messages):
                    last = self.messages[-1]
                    self.empty_tail = {"gen": last.get("gen", 0), "id": last["id"]}
                self.messages = self.messages[n:]
                self.start += n
                self._rewrite()
            return self.start, self.end


class DiskStorage:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.lock = threading.Lock()
        self.logs = {}  # (topic_id, partition) -> PartitionLog
        os.makedirs(data_dir, exist_ok=True)
        for topic_id in os.listdir(data_dir):
            tdir = os.path.join(data_dir, topic_id)
            if not os.path.isdir(tdir):
                continue
            for p in os.listdir(tdir):
                if p.isdigit():
                    self.logs[(topic_id, int(p))] = PartitionLog(os.path.join(tdir, p))

    def get(self, topic_id: str, partition: int, create: bool = True):
        if not _SAFE_ID.match(topic_id) or partition < 0:
            raise StorageError("bad_request", "Invalid topic id or partition")
        key = (topic_id, partition)
        with self.lock:
            log = self.logs.get(key)
            if log is None and create:
                log = PartitionLog(os.path.join(self.data_dir, topic_id, str(partition)))
                self.logs[key] = log
            return log

    def topic_ids(self):
        with self.lock:
            return sorted({tid for tid, _ in self.logs})

    def delete_topic(self, topic_id: str):
        if not _SAFE_ID.match(topic_id):
            raise StorageError("bad_request", "Invalid topic id")
        with self.lock:
            for key in [k for k in self.logs if k[0] == topic_id]:
                del self.logs[key]
            shutil.rmtree(os.path.join(self.data_dir, topic_id), ignore_errors=True)


storage_engine = DiskStorage(os.environ.get("STORAGE_DATA_DIR", "data/storage"))
