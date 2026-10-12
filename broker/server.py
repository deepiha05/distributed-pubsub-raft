# broker/server.py
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from broker.handlers import router as broker_router
from broker.node import raft, maintenance_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    raft.start()
    threading.Thread(target=maintenance_loop, args=(stop,), daemon=True).start()
    yield
    stop.set()
    raft.stop()


app = FastAPI(title="Broker API", lifespan=lifespan)
app.include_router(broker_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("broker.server:app", host="0.0.0.0", port=8000)
