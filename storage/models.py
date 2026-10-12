# storage/models.py
from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class StorageMessage(BaseModel):
    id: str
    topic: str
    partition: int
    offset: int
    payload: str
    timestamp: float
    key: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
    gen: int = 0  # generation of the leader that wrote it


class AppendRequest(BaseModel):
    epoch: int
    messages: List[StorageMessage]
    reset: bool = False
    prev_id: Optional[str] = None  # id that must precede the first message


class FenceRequest(BaseModel):
    epoch: int


class ExpireRequest(BaseModel):
    before: float
