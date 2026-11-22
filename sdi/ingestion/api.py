"""Ingestion API (FastAPI) - the single entry point into the staging layer.

  POST /batch/files                 register a landing file (called by the file-watcher)
  GET  /batch/files                 file registry / status
  POST /stream/{source}/events      micro-batch of streamed events (pharmacy & e-commerce sales)
  POST /stream/sessions/{id}/close  finalise a streaming session (throughput, latency summary)
  GET  /query/{query_id}            run an analytical query from the catalog (logged to ops.query_log)
  GET  /health

Run:  uvicorn sdi.ingestion.api:app --host 0.0.0.0 --port 8000
"""
import time
import datetime as dt
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ..db import pg_conn, pg_query
from ..sources import SOURCES, snake
from .validation import validate
from .batch_loader import register_file, insert_staging, write_rejects, known_medicines
from .airflow_trigger import trigger_dag

app = FastAPI(title="Pharma SDI Ingestion API", version="1.0")
_MED_CACHE = {"ts": 0.0, "ids": set()}


def medicines():
    if time.time() - _MED_CACHE["ts"] > 30:
        _MED_CACHE.update(ts=time.time(), ids=known_medicines())
    return _MED_CACHE["ids"]


class FileIn(BaseModel):
    path: str
    trigger_dag: bool = True


class EventBatch(BaseModel):
    session_id: str
    refresh_tag: Optional[str] = None
    events: List[dict]


@app.get("/health")
def health():
    pg_query("SELECT 1")
    return {"status": "ok", "time": dt.datetime.now(dt.timezone.utc).isoformat()}


@app.post("/batch/files")
def batch_file(f: FileIn):
    p = Path(f.path)
    if not p.exists():
        raise HTTPException(404, f"{p} not found")
    fid = register_file(p, detected_by="watcher")
    triggered = trigger_dag("sdi_01_batch_ingestion", {"file": str(p)}) if (fid and f.trigger_dag) else None
    return {"file_id": fid, "registered": fid is not None, "airflow_triggered": triggered}


@app.get("/batch/files")
def list_files(status: Optional[str] = None):
    sql = "SELECT file_id,file_name,status,refresh_tag,detected_at,processed_at FROM ops.file_registry"
    rows = pg_query(sql + (" WHERE status=%s" if status else "") + " ORDER BY file_id DESC LIMIT 200",
                    (status,) if status else None)
    return [dict(zip(["file_id", "file_name", "status", "refresh_tag", "detected_at", "processed_at"], r)) for r in rows]


@app.post("/stream/{source_key}/events")
def stream_events(source_key: str, batch: EventBatch):
    src = SOURCES.get(source_key)
    if src is None:
        raise HTTPException(404, "unknown source")
    received_at = dt.datetime.now(dt.timezone.utc)
    raw = pd.DataFrame(batch.events)
    created = pd.to_datetime(raw.pop("_event_created_at"), utc=True, errors="coerce") \
        if "_event_created_at" in raw else pd.Series(received_at, index=raw.index)
    raw = raw.astype("string")
    malformed_schema = 0
    missing = [c for c, _ in src.columns if c not in raw.columns]
    if missing:   # schema mismatch: whole micro-batch is malformed
        malformed_schema = len(raw)
    clean, rejected = validate(raw, src, medicines())
    with pg_conn() as c, c.cursor() as cur:
        inserted, lat_rows = 0, []
        if not clean.empty:
            clean = clean.copy()
            ev_created = pd.Series(created.loc[clean.index]).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            pk = snake(src.pk[0])
            inserted, lat_rows = insert_staging(cur, src, clean, batch.session_id, f"stream:{batch.session_id}",
                                                batch.refresh_tag or "STREAM", "stream", event_created=ev_created.values,
                                                returning=[pk, "_event_created_at", "_loaded_at"])
        write_rejects(cur, src, rejected, batch.session_id, "stream")
        dup = len(clean) - inserted
        if lat_rows:
            lat = [(batch.session_id, r[0], r[1], r[2], (r[2] - r[1]).total_seconds() * 1000) for r in lat_rows
                   if r[1] is not None]
            psycopg2.extras.execute_values(cur, "INSERT INTO ops.stream_event_latency VALUES %s", lat, page_size=1000)
            df = pd.DataFrame(lat, columns=["s", "e", "c", "l", "ms"])
            df["b"] = pd.to_datetime(df.l, utc=True).dt.floor("s")
            agg = df.groupby("b").agg(n=("ms", "size"), sm=("ms", "sum"), mx=("ms", "max")).reset_index()
            rows = [(batch.session_id, r.b.to_pydatetime(), int(r.n), 0, 0, float(r.sm), float(r.mx))
                    for r in agg.itertuples()]
        else:
            rows = []
        bucket = received_at.replace(microsecond=0)
        if len(rejected) or dup:
            rows.append((batch.session_id, bucket, 0, int(len(rejected)), int(dup), 0.0, 0.0))
        merged = {}
        for r in rows:   # one row per (session, second) so the upsert never touches a row twice
            k = (r[0], r[1])
            a = merged.get(k, [r[0], r[1], 0, 0, 0, 0.0, 0.0])
            merged[k] = [a[0], a[1], a[2] + r[2], a[3] + r[3], a[4] + r[4], a[5] + r[5], max(a[6], r[6])]
        rows = [tuple(v) for v in merged.values()]
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO ops.stream_metrics (session_id,bucket_ts,events_ingested,events_malformed,
                       events_duplicate,latency_sum_ms,latency_max_ms) VALUES %s
                ON CONFLICT (session_id,bucket_ts) DO UPDATE SET
                  events_ingested = ops.stream_metrics.events_ingested + EXCLUDED.events_ingested,
                  events_malformed = ops.stream_metrics.events_malformed + EXCLUDED.events_malformed,
                  events_duplicate = ops.stream_metrics.events_duplicate + EXCLUDED.events_duplicate,
                  latency_sum_ms = ops.stream_metrics.latency_sum_ms + EXCLUDED.latency_sum_ms,
                  latency_max_ms = GREATEST(ops.stream_metrics.latency_max_ms, EXCLUDED.latency_max_ms)""", rows)
    return {"received": len(raw), "ingested": inserted, "malformed": int(len(rejected)) + malformed_schema,
            "duplicates": dup}


@app.post("/stream/sessions/{session_id}/close")
def close_session(session_id: str, meta: dict):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("""
            WITH m AS (SELECT sum(events_ingested) ing, sum(events_malformed) bad, sum(events_duplicate) dup,
                              min(bucket_ts) t0, max(bucket_ts) t1, sum(latency_sum_ms) ls
                       FROM ops.stream_metrics WHERE session_id=%(s)s),
                 p AS (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) p95
                       FROM ops.stream_event_latency WHERE session_id=%(s)s)
            INSERT INTO ops.stream_session (session_id,refresh_tag,source_name,target_eps,start_ts,end_ts,duration_s,
                   events_sent,events_ingested,events_malformed,events_duplicate,throughput_eps,avg_latency_ms,
                   p95_latency_ms,status)
            SELECT %(s)s, %(tag)s, %(src)s, %(eps)s, %(t0)s, %(t1)s,
                   EXTRACT(EPOCH FROM (%(t1)s::timestamptz - %(t0)s::timestamptz)),
                   %(sent)s, m.ing, m.bad, m.dup,
                   m.ing / NULLIF(EXTRACT(EPOCH FROM (%(t1)s::timestamptz - %(t0)s::timestamptz)),0),
                   m.ls / NULLIF(m.ing,0), p.p95, %(status)s
            FROM m, p
            ON CONFLICT (session_id) DO NOTHING""",
                    dict(s=session_id, tag=meta.get("refresh_tag"), src=meta.get("source"), eps=meta.get("target_eps"),
                         t0=meta["start_ts"], t1=meta["end_ts"], sent=meta.get("events_sent"),
                         status=meta.get("status", "COMPLETED")))
    r = pg_query("SELECT events_ingested, events_malformed, throughput_eps, avg_latency_ms, p95_latency_ms "
                 "FROM ops.stream_session WHERE session_id=%s", (session_id,))
    return dict(zip(["ingested", "malformed", "throughput_eps", "avg_latency_ms", "p95_latency_ms"],
                    [float(x) if x is not None else None for x in r[0]])) if r else {}


@app.get("/query/{query_id}")
def run_query(query_id: str, tier: str = "star_optimized"):
    from ..query.runner import run_catalog_query
    res = run_catalog_query(query_id, tier=tier, caller="api")
    if res["status"] != "SUCCESS":
        raise HTTPException(400, res.get("error_message"))
    return res
