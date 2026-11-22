"""Database helpers for Postgres (staging + ops metrics) and DuckDB (SDI data mart)."""
import io
import json
import contextlib
import fcntl
import os
import time
import psycopg2
import psycopg2.extras
import duckdb
from .config import PG_DSN, DUCKDB_PATH, MART_DIR


@contextlib.contextmanager
def pg_conn(autocommit=False):
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def pg_exec(sql, params=None):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(sql, params)


def pg_query(sql, params=None):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


def pg_insert_dict(table, row: dict):
    cols = list(row)
    vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})"
    pg_exec(sql, vals)


def pg_copy_df(cur, df, table, columns):
    """Fast bulk load of a DataFrame into an existing table via COPY."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)


def duck_conn(read_only=False):
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


@contextlib.contextmanager
def mart_lock(timeout_s: int = 1800, poll_s: float = 2.0):
    """Cross-process lock for every task that opens the DuckDB mart (DuckDB allows one writer and no readers from
    other processes while it writes). Tasks WAIT for each other instead of failing with a lock error."""
    MART_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(MART_DIR / ".mart.lock"), os.O_CREAT | os.O_RDWR)
    deadline = time.time() + timeout_s
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() > deadline:
                    raise TimeoutError("timed out waiting for the DuckDB mart lock")
                time.sleep(poll_s)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def set_mart_state(status: str, run_id: str = None, cur=None):
    sql = ("INSERT INTO ops.mart_state (id, status, run_id, updated_at) VALUES (1, %s, %s, now()) "
           "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, run_id = EXCLUDED.run_id, updated_at = now()")
    if cur is not None:
        cur.execute(sql, (status, run_id))
    else:
        pg_exec(sql, (status, run_id))


def mart_state():
    r = pg_query("SELECT status, run_id, updated_at FROM ops.mart_state WHERE id = 1")
    return r[0] if r else ("READY", None, None)
