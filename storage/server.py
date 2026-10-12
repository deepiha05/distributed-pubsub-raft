# storage/server.py
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from storage.models import AppendRequest, FenceRequest, ExpireRequest
from storage.storage import storage_engine, StorageError
import uvicorn

app = FastAPI(title="Storage Node API")


@app.exception_handler(StorageError)
def storage_error_handler(request, exc: StorageError):
    status = 400 if exc.kind == "bad_request" else 409
    return JSONResponse(status_code=status, content=exc.to_dict())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/topics")
def list_topic_ids():
    return {"topic_ids": storage_engine.topic_ids()}


@app.post("/store/{topic_id}/{partition}")
def append_messages(topic_id: str, partition: int, req: AppendRequest):
    log = storage_engine.get(topic_id, partition)
    end = log.append(req.epoch, [m.model_dump() for m in req.messages], reset=req.reset,
                     prev_id=req.prev_id)
    return {"status": "Messages stored", "end": end}


@app.get("/store/{topic_id}/{partition}")
def get_messages(topic_id: str, partition: int, offset: int = 0, limit: int = 10):
    log = storage_engine.get(topic_id, partition, create=False)
    if log is None:
        return {"messages": [], "start": 0, "end": 0, "last_gen": 0}
    messages, start, end, last_gen = log.read(offset, limit)
    return {"messages": messages, "start": start, "end": end, "last_gen": last_gen}


@app.get("/store/{topic_id}/{partition}/offset")
def get_offset(topic_id: str, partition: int):
    log = storage_engine.get(topic_id, partition, create=False)
    if log is None:
        return {"start": 0, "end": 0, "epoch": 0, "last_gen": 0, "last_id": None}
    return log.info()


@app.post("/store/{topic_id}/{partition}/fence")
def fence(topic_id: str, partition: int, req: FenceRequest):
    return storage_engine.get(topic_id, partition).fence(req.epoch)


@app.post("/store/{topic_id}/{partition}/expire")
def expire(topic_id: str, partition: int, req: ExpireRequest):
    log = storage_engine.get(topic_id, partition, create=False)
    if log is None:
        return {"start": 0, "end": 0}
    start, end = log.expire(req.before)
    return {"start": start, "end": end}


@app.delete("/store/{topic_id}")
def delete_topic(topic_id: str):
    storage_engine.delete_topic(topic_id)
    return {"status": "Topic data deleted", "topic_id": topic_id}


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    uvicorn.run("storage.server:app", host="0.0.0.0", port=port)
