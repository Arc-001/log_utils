import datetime
import threading
import uuid

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from common import config, mq

app = FastAPI()

_lock = threading.Lock()
_connection = None
_channel = None


class IngestRequest(BaseModel):
    source: str
    lines: list[str]


def _publish(payload: dict) -> None:
    global _connection, _channel
    with _lock:
        # Idle connections get reset by the broker between requests (missed
        # heartbeats), so reconnect once on failure rather than trusting
        # is_closed, which only reflects state as of the last operation.
        for attempt in range(2):
            try:
                if _connection is None or _connection.is_closed:
                    _connection = mq.get_connection()
                    _channel = _connection.channel()
                mq.publish_json(_channel, config.RAW_QUEUE, payload)
                return
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError):
                _connection = None
                _channel = None
                if attempt == 1:
                    raise


@app.post("/ingest")
def ingest(req: IngestRequest):
    if not req.lines:
        raise HTTPException(status_code=400, detail="lines must not be empty")

    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "source": req.source,
        "lines": req.lines,
        "received_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    _publish(payload)
    return {"job_id": job_id, "accepted": len(req.lines)}


@app.get("/health")
def health():
    return {"status": "ok"}
