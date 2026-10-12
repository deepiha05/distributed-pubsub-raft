# broker/models.py
from pydantic import BaseModel, Field
from typing import List, Optional


class Topic(BaseModel):
    name: str = Field(min_length=1)
    partitions: int = Field(ge=1)
    retention_seconds: int = 0  # 0 or less keeps messages forever
    replica_factor: int = Field(default=3, ge=1)


class PublishRequest(BaseModel):
    payload: str = Field(min_length=1)
    key: Optional[str] = None


class RetentionUpdate(BaseModel):
    retention_seconds: int


class Subscription(BaseModel):
    id: str = ""
    subscriber_id: str
    topic: str
    # Dead-letter policy: after max_delivery_attempts deliveries without an ack,
    # a message is moved to dead_letter_topic. 0 = no limit (default 5 when a topic is set).
    dead_letter_topic: Optional[str] = None
    max_delivery_attempts: int = Field(default=0, ge=0)


class DeadLetterPolicy(BaseModel):
    dead_letter_topic: Optional[str] = None  # None removes the policy
    max_delivery_attempts: int = Field(default=5, ge=1)


class Ack(BaseModel):
    partition: int
    offset: int


class AckRequest(BaseModel):
    acks: List[Ack] = Field(min_length=1)


class SeekRequest(BaseModel):
    partition: int
    offset: int


class JoinRequest(BaseModel):
    consumer_id: str = Field(min_length=1)
