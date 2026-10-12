# broker/metadata.py
"""
The replicated state machine: topics, subscriptions, consumer-group members and
committed offsets. It only changes through MetadataStore.apply(), which Raft
calls with each committed command in log order, so every broker ends up with
identical metadata.
"""
import copy
import threading


class CommandError(Exception):
    status_code = 400


class NotFoundError(CommandError):
    status_code = 404


class MetadataStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.topics = {}         # topic name -> topic dict
        self.subscriptions = {}  # subscription id -> subscription dict

    def apply(self, command):
        handler = getattr(self, "_apply_" + command["op"], None)
        if handler is None:
            raise CommandError(f"Unknown command {command['op']}")
        with self.lock:
            return copy.deepcopy(handler(command))

    # ----- commands -----

    def _apply_noop(self, c):
        return None

    def _apply_create_topic(self, c):
        t = c["topic"]
        if t["name"] in self.topics:
            raise CommandError("Topic already exists")
        self.topics[t["name"]] = dict(t)
        return self.topics[t["name"]]

    def _topic(self, name):
        if name not in self.topics:
            raise NotFoundError("Topic doesn't exist")
        return self.topics[name]

    def _apply_update_retention(self, c):
        topic = self._topic(c["name"])
        topic["retention_seconds"] = c["retention_seconds"]
        return topic

    def _apply_delete_topic(self, c):
        topic = self._topic(c["name"])
        del self.topics[c["name"]]
        subs = [sid for sid, s in self.subscriptions.items() if s["topic"] == c["name"]]
        for sid in subs:
            del self.subscriptions[sid]
        for s in self.subscriptions.values():
            if s.get("dead_letter_topic") == c["name"]:
                s["dead_letter_topic"], s["max_delivery_attempts"] = None, 0
        return {"topic": topic, "deleted_subscriptions": subs}

    def _apply_delete_all_topics(self, c):
        topics = list(self.topics.values())
        self.topics.clear()
        self.subscriptions.clear()
        return {"topics": topics}

    def _sub(self, sid):
        if sid not in self.subscriptions:
            raise NotFoundError("Subscription not found")
        return self.subscriptions[sid]

    def _apply_create_subscription(self, c):
        s = c["subscription"]
        if s["id"] in self.subscriptions:
            raise CommandError("Subscription already exists")
        topic = self._topic(s["topic"])
        sub = {"id": s["id"], "subscriber_id": s["subscriber_id"], "topic": s["topic"],
               "offsets": {p: 0 for p in range(topic["partitions"])}, "members": [],
               "dead_letter_topic": None, "max_delivery_attempts": 0}
        self._set_dead_letter(sub, s.get("dead_letter_topic"), s.get("max_delivery_attempts", 0))
        self.subscriptions[sub["id"]] = sub
        return sub

    def _set_dead_letter(self, sub, dl_topic, max_attempts):
        if dl_topic is None:
            sub["dead_letter_topic"], sub["max_delivery_attempts"] = None, 0
            return
        self._topic(dl_topic)
        if dl_topic == sub["topic"]:
            raise CommandError("Dead-letter topic must differ from the subscribed topic")
        sub["dead_letter_topic"], sub["max_delivery_attempts"] = dl_topic, max_attempts or 5

    def _apply_set_dead_letter_policy(self, c):
        sub = self._sub(c["id"])
        self._set_dead_letter(sub, c["dead_letter_topic"], c["max_delivery_attempts"])
        return sub

    def _apply_delete_subscription(self, c):
        return self.subscriptions.pop(self._sub(c["id"])["id"])

    def _apply_commit_offsets(self, c):
        """Acknowledge: offsets only move forward."""
        sub = self._sub(c["id"])
        for p, off in c["offsets"].items():
            p = int(p)
            if p in sub["offsets"]:
                sub["offsets"][p] = max(sub["offsets"][p], off)
        return sub

    def _apply_seek(self, c):
        """Replay: move a partition's offset anywhere, including backwards."""
        sub = self._sub(c["id"])
        p = int(c["partition"])
        if p not in sub["offsets"]:
            raise CommandError("Partition out of range")
        sub["offsets"][p] = max(0, c["offset"])
        return sub

    def _apply_join_group(self, c):
        sub = self._sub(c["id"])
        if c["consumer_id"] not in sub["members"]:
            sub["members"].append(c["consumer_id"])
            sub["members"].sort()
        return sub

    def _apply_leave_group(self, c):
        sub = self._sub(c["id"])
        if c["consumer_id"] not in sub["members"]:
            raise NotFoundError("Consumer is not a member of this subscription")
        sub["members"].remove(c["consumer_id"])
        return sub

    # ----- reads -----

    def get_topic(self, name):
        with self.lock:
            return copy.deepcopy(self.topics.get(name))

    def list_topics(self):
        with self.lock:
            return copy.deepcopy(list(self.topics.values()))

    def get_subscription(self, sid):
        with self.lock:
            return copy.deepcopy(self.subscriptions.get(sid))

    def list_subscriptions(self):
        with self.lock:
            return copy.deepcopy(list(self.subscriptions.values()))


def assigned_partitions(sub, consumer_id, partitions):
    """
    Consumer-group assignment: each partition goes to exactly one member
    (round-robin over the sorted member list). Without members, a single
    consumer reads every partition.
    """
    members = sub["members"]
    if not members:
        return list(range(partitions))
    if consumer_id not in members:
        return None
    i = members.index(consumer_id)
    return [p for p in range(partitions) if p % len(members) == i]
