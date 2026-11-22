"""Streaming producer: replays a generated sales increment as a live event stream into the ingestion API.

Each event gets `_event_created_at` = the moment the producer creates it (simulated source-row creation time).
The API stamps the DB commit time, so:   end-to-end latency = commit_ts - _event_created_at
Throughput is throttled to a target events/second and sent in micro-batches (default 50 events / POST).
"""
import time
import shutil
import datetime as dt
from pathlib import Path

import pandas as pd
import requests

from ..config import INGEST_API_URL, STREAM_EVENTS_PER_SEC, STREAM_MICROBATCH, STREAM_QUEUE_DIR, ARCHIVE_DIR


def iso_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def stream_file(path: Path, eps: int = STREAM_EVENTS_PER_SEC, microbatch: int = STREAM_MICROBATCH,
                api: str = INGEST_API_URL, source: str = "05_pharmacy_sales_data", archive=True) -> dict:
    path = Path(path)
    tag = path.name.split("_")[0]
    session = f"S-{tag}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}"
    df = pd.read_excel(path, dtype=str, engine="calamine")
    records = df.astype(object).where(df.notna(), None).to_dict("records")
    http = requests.Session()
    start = iso_now()
    t0 = time.perf_counter()
    sent = ingested = malformed = 0
    for i in range(0, len(records), microbatch):
        chunk = records[i:i + microbatch]
        now = iso_now()
        for r in chunk:
            r["_event_created_at"] = now
        resp = http.post(f"{api}/stream/{source}/events",
                         json={"session_id": session, "refresh_tag": tag, "events": chunk}, timeout=60)
        resp.raise_for_status()
        out = resp.json()
        sent += len(chunk)
        ingested += out["ingested"]
        malformed += out["malformed"]
        target = (i + len(chunk)) / eps          # throttle to the target rate
        lag = target - (time.perf_counter() - t0)
        if lag > 0:
            time.sleep(lag)
    summary = http.post(f"{api}/stream/sessions/{session}/close",
                        json={"refresh_tag": tag, "source": source, "target_eps": eps, "start_ts": start,
                              "end_ts": iso_now(), "events_sent": sent}, timeout=60).json()
    if archive:
        target = ARCHIVE_DIR / tag / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), target)
    return {"session_id": session, "events_sent": sent, "ingested": ingested, "malformed": malformed, **summary}


def stream_pending(**kw) -> list:
    return [stream_file(p, **kw) for p in sorted(STREAM_QUEUE_DIR.glob("*.xlsx"))]


if __name__ == "__main__":
    for r in stream_pending():
        print(r)
