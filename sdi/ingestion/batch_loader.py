"""Batch ingestion: Excel/CSV landing files -> Postgres staging (ODS).

Per source sheet the loader measures (-> ops.batch_load_log -> Grafana 'Batch Ingestion'):
  Batch load duration  = end_ts - start_ts  (read + validate + insert)
  Rows processed       = len(df) read from the file, rows loaded, rows rejected, duplicates skipped
  Success / failure    = whether the load finished without an unhandled exception
"""
import shutil
import time
import uuid
import json
import datetime as dt
from pathlib import Path

import pandas as pd
import psycopg2.extras

from ..config import LANDING_DIR, ARCHIVE_DIR, QUARANTINE_DIR
from ..db import pg_conn, pg_copy_df, pg_query
from ..sources import SOURCES, Source, snake, source_for_name
from .validation import validate

META = ["_batch_id", "_source_file", "_refresh_tag", "_ingest_mode"]


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def refresh_tag_from_name(name: str) -> str:
    for tag in ("R1", "R2", "R3", "R4"):
        if name.startswith(tag + "_") or f"/{tag}/" in name:
            return tag
    return "BASE"


def known_medicines(extra: pd.DataFrame = None) -> set:
    ids = {r[0] for r in pg_query("SELECT DISTINCT medicine_id FROM staging.stg_master_data")}
    if extra is not None and "MedicineID" in extra:
        ids |= set(extra["MedicineID"].dropna().astype(str))
    return ids


def iter_file_frames(path: Path):
    """Yields (Source, raw string DataFrame, sheet label, read_seconds) for every recognised sheet/file."""
    if path.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
        t0 = time.perf_counter()
        xl = pd.ExcelFile(path, engine="calamine")
        open_s = time.perf_counter() - t0
        sheets = [(s, source_for_name(s.strip()) or source_for_name(path.name)) for s in xl.sheet_names]
        sheets = [(s, src) for s, src in sheets if src]
        for s, src in sorted(sheets, key=lambda x: x[1].load_order):
            t1 = time.perf_counter()
            df = xl.parse(s, dtype=str)
            yield src, df, f"{path.name}[{s.strip()}]", time.perf_counter() - t1 + open_s / max(len(sheets), 1)
    elif path.suffix.lower() == ".csv":
        src = source_for_name(path.name)
        t1 = time.perf_counter()
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        yield src, df, path.name, time.perf_counter() - t1


def insert_staging(cur, src: Source, clean: pd.DataFrame, batch_id, source_file, refresh_tag, ingest_mode,
                   event_created=None, returning=None):
    """COPY into a temp table, then INSERT .. ON CONFLICT DO NOTHING (idempotent re-runs).
    Returns rows inserted, or (rows inserted, returned rows) when `returning` is a column list."""
    if clean.empty:
        return (0, []) if returning else 0
    df = clean.copy()
    df["_batch_id"], df["_source_file"], df["_refresh_tag"], df["_ingest_mode"] = (
        batch_id, source_file, refresh_tag, ingest_mode)
    cols = list(df.columns)
    if event_created is not None:
        df["_event_created_at"] = event_created
        cols.append("_event_created_at")
    tmp = f"tmp_{src.table}_{uuid.uuid4().hex[:8]}"
    cur.execute(f"CREATE TEMP TABLE {tmp} (LIKE staging.{src.table} INCLUDING DEFAULTS) ON COMMIT DROP")
    pg_copy_df(cur, df[cols], tmp, cols)
    cl = ",".join(cols)
    ret = f" RETURNING {','.join(returning)}" if returning else ""
    cur.execute(f"INSERT INTO staging.{src.table} ({cl}) SELECT {cl} FROM {tmp} ORDER BY _stg_id "
                f"ON CONFLICT DO NOTHING{ret}")
    if returning:
        rows = cur.fetchall()
        return len(rows), rows
    return cur.rowcount


def write_rejects(cur, src: Source, rejected: pd.DataFrame, batch_id, ingest_mode):
    if rejected.empty:
        return
    rows = [(src.key, batch_id, ingest_mode, r.reason_code, r.reason_detail, json.dumps(r.record, default=str))
            for r in rejected.itertuples()]
    psycopg2.extras.execute_values(
        cur, "INSERT INTO rejects.rejected_rows (source_name,batch_id,ingest_mode,reason_code,reason_detail,record) "
             "VALUES %s", rows, page_size=1000)


def load_frame(src: Source, raw: pd.DataFrame, label: str, read_s: float, batch_id: str, refresh_tag: str,
               dag_run_id: str = None, file_size_mb: float = None) -> dict:
    start = now_utc()
    t0 = time.perf_counter()
    log = dict(batch_id=batch_id, dag_run_id=dag_run_id, refresh_tag=refresh_tag, source_name=src.key,
               domain=src.domain, file_name=label, file_size_mb=file_size_mb, rows_read=len(raw), read_s=read_s)
    try:
        meds = known_medicines(raw if src.key == "01_master_data" else None)
        clean, rejected = validate(raw, src, meds)
        log["validate_s"] = time.perf_counter() - t0
        t1 = time.perf_counter()
        with pg_conn() as c, c.cursor() as cur:
            inserted = insert_staging(cur, src, clean, batch_id, label, refresh_tag, "batch")
            write_rejects(cur, src, rejected, batch_id, "batch")
        log["insert_s"] = time.perf_counter() - t1
        log.update(rows_loaded=inserted, rows_rejected=len(rejected), rows_duplicate=len(clean) - inserted,
                   status="SUCCESS", error_message=None)
    except Exception as e:  # recorded, re-raised by caller policy
        log.update(rows_loaded=0, rows_rejected=0, rows_duplicate=0, status="FAILED", error_message=str(e)[:2000])
    end = now_utc()
    log["start_ts"] = start - dt.timedelta(seconds=read_s)   # duration includes reading the sheet
    log["end_ts"] = end
    log["duration_s"] = (end - log["start_ts"]).total_seconds()
    log["rows_per_sec"] = round(log["rows_read"] / log["duration_s"], 1) if log["duration_s"] else None
    cols = list(log)
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(f"INSERT INTO ops.batch_load_log ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})",
                    [log[k] for k in cols])
    return log


def process_file(path: Path, dag_run_id: str = None, archive: bool = True) -> list:
    """Loads every recognised sheet of one landing file. Moves the file to archive/ (or quarantine/)."""
    path = Path(path)
    batch_id = f"B-{now_utc():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    tag = refresh_tag_from_name(path.name)
    size_mb = round(path.stat().st_size / 1e6, 3)
    _set_status(path, "PROCESSING")
    logs = []
    for src, raw, label, read_s in iter_file_frames(path):
        logs.append(load_frame(src, raw, label, read_s, batch_id, tag, dag_run_id, size_mb))
    ok = logs and all(l["status"] == "SUCCESS" for l in logs)
    _set_status(path, "LOADED" if ok else "FAILED")
    if archive:
        target = (ARCHIVE_DIR if ok else QUARANTINE_DIR) / tag / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():   # same file re-delivered (e.g. generator re-run): keep both, file_path must stay unique
            target = target.with_name(f"{path.stem}.{batch_id}{path.suffix}")
        shutil.move(str(path), target)
        with pg_conn() as c, c.cursor() as cur:
            cur.execute("UPDATE ops.file_registry SET file_path=%s WHERE file_path=%s", (str(target), str(path)))
    return logs


def register_file(path: Path, detected_by="dag"):
    path = Path(path)
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO ops.file_registry (file_name,file_path,file_size_mb,refresh_tag,detected_by)
                       VALUES (%s,%s,%s,%s,%s) ON CONFLICT (file_path) DO NOTHING RETURNING file_id""",
                    (path.name, str(path), round(path.stat().st_size / 1e6, 3), refresh_tag_from_name(path.name),
                     detected_by))
        r = cur.fetchone()
    return r[0] if r else None


def _set_status(path: Path, status: str):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE ops.file_registry SET status=%s, processed_at=CASE WHEN %s IN ('LOADED','FAILED') "
                    "THEN now() END WHERE file_path=%s", (status, status, str(path)))


def pending_files() -> list:
    """Files the watcher registered + any landing file not yet registered (safety net if the watcher is down)."""
    for p in LANDING_DIR.rglob("*"):
        if p.is_file() and p.suffix.lower() in (".xlsx", ".csv") and not p.name.startswith("~$"):
            register_file(p, "dag")
    rows = pg_query("SELECT file_path FROM ops.file_registry WHERE status='PENDING' ORDER BY detected_at, file_id")
    paths = [Path(r[0]) for r in rows if Path(r[0]).exists()]
    # master data must load first (referential checks), then by source load order
    def order(p):
        s = source_for_name(p.name)
        return (0 if "master" in p.name or "Source_Data" in p.name else 1, s.load_order if s else 0, p.name)
    return sorted(paths, key=order)


def process_pending(dag_run_id: str = None) -> list:
    out = []
    for p in pending_files():
        out.extend(process_file(p, dag_run_id))
    return out
