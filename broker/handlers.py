# broker/handlers.py
"""
Client API. Any broker accepts any request: a follower forwards it to the
current Raft leader, which orders all metadata changes and assigns offsets.
"""
import hashlib
import itertools
import logging
import random
import threading
import time
import uuid
from typing import Optional

import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from broker import config
from broker.metadata import CommandError, assigned_partitions
from broker.models import (Topic, PublishRequest, RetentionUpdate, Subscription,
                           AckRequest, SeekRequest, JoinRequest, DeadLetterPolicy)
from broker.node import raft, metadata, storage, deliveries
from broker.raft import NotLeaderError
from broker.storage_client import QuorumError, StaleEpochError

log = logging.getLogger("broker")
router = APIRouter()
FORWARD_HEADER = "X-Forwarded-By-Broker"
_pull_rotation = itertools.count()
_pull_locks_lock = threading.Lock()
_pull_locks = {}  # subscription id -> lock; one pull at a time per subscription


def _pull_lock(sid):
    with _pull_locks_lock:
        return _pull_locks.setdefault(sid, threading.Lock())


def leader_or_forward(request: Request, body=None):
    """None if this broker should handle the request, else the leader's response."""
    if raft.is_leader():
        if not raft.leader_ready():
            raise HTTPException(503, "Leader elected but still catching up; retry shortly")
        return None
    if request.headers.get(FORWARD_HEADER):
        raise HTTPException(503, "Leadership is changing; retry shortly")
    url = raft.leader_url()
    if not url:
        raise HTTPException(503, "No leader elected yet (a majority of brokers must be up)")
    try:
        r = requests.request(request.method, url + request.url.path, params=request.query_params,
                             json=body, headers={FORWARD_HEADER: config.BROKER_ID}, timeout=10)
    except requests.RequestException as e:
        raise HTTPException(503, f"Leader {raft.status()['leader']} unreachable: {e}")
    try:
        content = r.json()
    except ValueError:
        content = {"detail": r.text}
    return JSONResponse(status_code=r.status_code, content=content)


def propose(command):
    try:
        return raft.propose(command)
    except CommandError as e:
        raise HTTPException(e.status_code, str(e))
    except NotLeaderError:
        raise HTTPException(503, "Leadership changed while processing the request; retry")
    except TimeoutError as e:
        raise HTTPException(503, str(e))


def require_topic(name):
    topic = metadata.get_topic(name)
    if topic is None:
        raise HTTPException(404, "Topic not found")
    return topic


def require_subscription(sid):
    sub = metadata.get_subscription(sid)
    if sub is None:
        raise HTTPException(404, "Subscription not found")
    return sub


# ----- cluster -----

@router.get("/health")
def health():
    return raft.status()


@router.get("/cluster")
def cluster():
    return {"broker": raft.status(), "storage_nodes": storage.nodes}


# ----- topics -----

@router.post("/topics")
def create_topic(topic: Topic, request: Request):
    if (fwd := leader_or_forward(request, topic.model_dump())) is not None:
        return fwd
    t = topic.model_dump()
    t["id"] = uuid.uuid4().hex
    t["replica_factor"] = min(t["replica_factor"], len(storage.nodes))
    t["created_at"] = time.time()
    return {"status": "Topic created", "topic": propose({"op": "create_topic", "topic": t})}


@router.get("/topics")
def list_topics(request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    return {"topics": metadata.list_topics()}


@router.put("/topics/{topic}/retention")
def update_retention(topic: str, body: RetentionUpdate, request: Request):
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    t = propose({"op": "update_retention", "name": topic, "retention_seconds": body.retention_seconds})
    return {"status": "Retention updated", "topic": topic, "retention_seconds": t["retention_seconds"]}


@router.delete("/topics/{topic}")
def delete_topic(topic: str, request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    result = propose({"op": "delete_topic", "name": topic})
    storage.delete_topic_data(result["topic"]["id"])
    return {"status": "Topic deleted", "topic": topic,
            "deleted_subscriptions": result["deleted_subscriptions"]}


@router.delete("/topics")
def delete_all_topics(request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    result = propose({"op": "delete_all_topics"})
    for t in result["topics"]:
        storage.delete_topic_data(t["id"])
    return {"status": "All topics deleted", "count": len(result["topics"])}


# ----- publishing -----

def append_message(t, payload, key=None, attributes=None):
    """Write one message to a topic (leader only). Raises storage/leadership errors."""
    if key:
        # Same key -> same partition, so messages with one key stay in order.
        partition = int(hashlib.sha256(key.encode()).hexdigest(), 16) % t["partitions"]
    else:
        partition = random.randrange(t["partitions"])
    message = {"id": str(uuid.uuid4()), "topic": t["name"], "partition": partition,
               "payload": payload, "timestamp": time.time(), "key": key, "attributes": attributes}
    result = storage.publish(t, partition, message, raft.leader_epoch())
    return {"id": message["id"], "partition": partition, **result}


@router.post("/topics/{topic}/messages")
def publish_message(topic: str, body: PublishRequest, request: Request):
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    t = require_topic(topic)
    try:
        result = append_message(t, body.payload, body.key)
    except (NotLeaderError, StaleEpochError) as e:
        raise HTTPException(503, f"Leadership changed; retry ({e})")
    except QuorumError as e:
        raise HTTPException(503, f"Failed to achieve quorum: {e}")
    return {"status": "Message published", **result}


# ----- subscriptions and consumer groups -----

@router.post("/subscriptions")
def create_subscription(sub: Subscription, request: Request):
    if (fwd := leader_or_forward(request, sub.model_dump())) is not None:
        return fwd
    s = sub.model_dump()
    s["id"] = s["id"] or str(uuid.uuid4())
    return {"status": "Subscription created",
            "subscription": propose({"op": "create_subscription", "subscription": s})}


@router.get("/subscriptions")
def list_subscriptions(request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    return {"subscriptions": metadata.list_subscriptions()}


@router.get("/subscriptions/{subscription_id}")
def get_subscription(subscription_id: str, request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    return require_subscription(subscription_id)


@router.delete("/subscriptions/{subscription_id}")
def delete_subscription(subscription_id: str, request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    propose({"op": "delete_subscription", "id": subscription_id})
    return {"status": "Subscription deleted", "subscription": subscription_id}


@router.put("/subscriptions/{subscription_id}/dead-letter")
def set_dead_letter_policy(subscription_id: str, body: DeadLetterPolicy, request: Request):
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    sub = propose({"op": "set_dead_letter_policy", "id": subscription_id,
                   "dead_letter_topic": body.dead_letter_topic,
                   "max_delivery_attempts": body.max_delivery_attempts})
    return {"status": "Dead-letter policy updated", "dead_letter_topic": sub["dead_letter_topic"],
            "max_delivery_attempts": sub["max_delivery_attempts"]}


@router.post("/subscriptions/{subscription_id}/members")
def join_group(subscription_id: str, body: JoinRequest, request: Request):
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    sub = propose({"op": "join_group", "id": subscription_id, "consumer_id": body.consumer_id})
    t = require_topic(sub["topic"])
    return {"status": "Joined consumer group", "members": sub["members"],
            "assigned_partitions": assigned_partitions(sub, body.consumer_id, t["partitions"])}


@router.delete("/subscriptions/{subscription_id}/members/{consumer_id}")
def leave_group(subscription_id: str, consumer_id: str, request: Request):
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    sub = propose({"op": "leave_group", "id": subscription_id, "consumer_id": consumer_id})
    return {"status": "Left consumer group", "members": sub["members"]}


@router.get("/subscriptions/{subscription_id}/messages")
def pull_messages(subscription_id: str, request: Request, limit: int = 10,
                  consumer_id: Optional[str] = None):
    """
    Return up to `limit` messages after the committed offsets. Messages are
    redelivered on every pull until acknowledged (at-least-once delivery), or,
    with a dead-letter policy, until they reach max_delivery_attempts.
    """
    if (fwd := leader_or_forward(request)) is not None:
        return fwd
    with _pull_lock(subscription_id):
        return _pull(subscription_id, limit, consumer_id)


def _dead_letter(sub, partition, batch, dlq):
    """
    Move messages at the head of `batch` that used up their delivery attempts to
    the dead-letter topic, then commit past them. Only the head needs checking:
    pulls read contiguously from the committed offset, so a message is never
    delivered more often than the ones before it. Returns how many were moved.
    """
    sid, limit = sub["id"], sub["max_delivery_attempts"]
    moved = 0
    while moved < len(batch) and deliveries.count(sid, partition, batch[moved]["offset"]) >= limit:
        m = batch[moved]
        attrs = dict(m.get("attributes") or {})
        attrs.update({"source_topic": sub["topic"], "source_subscription": sid,
                      "source_partition": partition, "source_offset": m["offset"],
                      "source_message_id": m["id"],
                      "delivery_attempts": deliveries.count(sid, partition, m["offset"])})
        try:
            append_message(dlq, m["payload"], m.get("key"), attrs)
        except (NotLeaderError, StaleEpochError, QuorumError) as e:
            # Keep delivering it rather than lose it; retried on the next pull.
            log.warning("Dead-lettering %s/%d@%d failed: %s", sub["topic"], partition, m["offset"], e)
            break
        moved += 1
    if moved:
        next_offset = batch[moved - 1]["offset"] + 1
        propose({"op": "commit_offsets", "id": sid, "offsets": {str(partition): next_offset}})
        deliveries.acked(sid, partition, next_offset)
    return moved


def _pull(subscription_id, limit, consumer_id):
    sub = require_subscription(subscription_id)
    t = require_topic(sub["topic"])
    dlq = None
    if sub.get("dead_letter_topic") and sub.get("max_delivery_attempts"):
        dlq = metadata.get_topic(sub["dead_letter_topic"])
    partitions = assigned_partitions(sub, consumer_id, t["partitions"])
    if partitions is None:
        raise HTTPException(400, "This subscription is a consumer group; join it and pass consumer_id")
    if partitions:
        # Rotate the starting partition so one busy partition cannot starve the others.
        k = next(_pull_rotation) % len(partitions)
        partitions = partitions[k:] + partitions[:k]
    messages, unavailable, dead_lettered = [], [], 0
    for p in partitions:
        if len(messages) >= limit:
            break
        try:
            offset = sub["offsets"].get(p, 0)
            while True:
                batch = storage.read(t, p, offset, limit - len(messages), raft.leader_epoch())
                moved = _dead_letter(sub, p, batch, dlq) if dlq is not None else 0
                if not moved:
                    break
                # Read past the dead-lettered messages so the pull is still filled.
                dead_lettered += moved
                offset = batch[moved - 1]["offset"] + 1
        except (QuorumError, StaleEpochError):
            unavailable.append(p)
            continue
        except NotLeaderError:
            raise HTTPException(503, "Leadership changed; retry")
        for m in batch:
            m["delivery_attempts"] = deliveries.bump(subscription_id, p, m["offset"])
        messages.extend(batch)
    return {"messages": messages, "unavailable_partitions": unavailable,
            "dead_lettered": dead_lettered}


@router.post("/subscriptions/{subscription_id}/ack")
def acknowledge_messages(subscription_id: str, body: AckRequest, request: Request):
    """Acknowledging offset N on a partition commits everything up to and including N."""
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    offsets = {}
    for a in body.acks:
        offsets[str(a.partition)] = max(offsets.get(str(a.partition), 0), a.offset + 1)
    sub = propose({"op": "commit_offsets", "id": subscription_id, "offsets": offsets})
    for p, off in offsets.items():
        deliveries.acked(subscription_id, int(p), off)
    return {"status": "Acknowledged messages", "subscription": subscription_id,
            "offsets": sub["offsets"]}


@router.post("/subscriptions/{subscription_id}/seek")
def seek(subscription_id: str, body: SeekRequest, request: Request):
    """Replay (or skip) by moving a partition's offset."""
    if (fwd := leader_or_forward(request, body.model_dump())) is not None:
        return fwd
    sub = propose({"op": "seek", "id": subscription_id, "partition": body.partition,
                   "offset": body.offset})
    return {"status": "Offset moved", "offsets": sub["offsets"]}


# ----- Raft RPCs between brokers -----

@router.post("/raft/request_vote")
def raft_request_vote(args: dict):
    return raft.handle_request_vote(args)


@router.post("/raft/append_entries")
def raft_append_entries(args: dict):
    return raft.handle_append_entries(args)
