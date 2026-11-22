"""Query execution layer: timed wrapper, optimisation step, benchmark, result-equivalence check.

Every execution is logged to ops.query_log (-> Grafana 'Query Layer' dashboard):
  Query execution time  = time.perf_counter() around execute + fetch
  Daily active queries  = count(*) per day (benchmark + dashboard refresh simulation + API + ad-hoc)
  Query error rate      = failed / total
"""
import time
import shutil
import uuid
import datetime as dt

import duckdb
import psycopg2
import psycopg2.extras

from ..config import PG_DSN, DUCKDB_PATH, PARQUET_DIR
import contextlib
from ..db import pg_conn, mart_lock, mart_state
from .catalog import QUERIES, BY_ID, ADHOC

@contextlib.contextmanager
def ready_mart(timeout_s: int = 1800, poll_s: int = 15):
    """Holds the mart lock while the mart is READY (watermark committed). If a transform is mid-load (LOADING), the
    lock is released so the transform can finish, and we retry - so a benchmark never compares half-loaded data."""
    deadline = time.time() + timeout_s
    while True:
        with mart_lock(timeout_s):
            status, run, ts = mart_state()
            if status == "READY":
                yield
                return
        if time.time() > deadline:
            raise TimeoutError(f"mart still {status} (run {run}, since {ts}); a transform run probably failed - "
                               "re-run sdi_03_transform")
        time.sleep(poll_s)


ENGINE = {"oltp_staging": "postgres", "star_schema": "duckdb", "star_optimized": "duckdb", "lake_parquet": "duckdb"}


def _log(rows):
    with pg_conn() as c, c.cursor() as cur:
        psycopg2.extras.execute_values(cur, """INSERT INTO ops.query_log (executed_at, run_id, refresh_tag, caller,
            query_id, widget_name, business_question, tier, engine, exec_ms, rows_returned, status, error_message)
            VALUES %s""", rows)


def _execute(engine, sql, duck=None, pg=None):
    t0 = time.perf_counter()
    if engine == "duckdb":
        res = duck.execute(sql).fetchall()
    else:
        with pg.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchall()
    return res, (time.perf_counter() - t0) * 1000


def run_catalog_query(query_id, tier="star_optimized", caller="api", run_id=None, refresh_tag=None, log=True,
                      duck=None, pg=None):
    if ENGINE[tier] == "duckdb" and duck is None:
        with mart_lock():
            return _run_catalog_query(query_id, tier, caller, run_id, refresh_tag, log, None, pg)
    return _run_catalog_query(query_id, tier, caller, run_id, refresh_tag, log, duck, pg)


def _run_catalog_query(query_id, tier, caller, run_id, refresh_tag, log, duck, pg):
    q = BY_ID[query_id]
    engine = ENGINE[tier]
    own_duck = own_pg = False
    if engine == "duckdb" and duck is None:
        duck, own_duck = duckdb.connect(str(DUCKDB_PATH), read_only=True), True
    if engine == "postgres" and pg is None:
        pg, own_pg = psycopg2.connect(PG_DSN), True
    status, err, rows, ms = "SUCCESS", None, [], None
    started = dt.datetime.now(dt.timezone.utc)
    try:
        rows, ms = _execute(engine, q["sql"][tier], duck, pg)
    except Exception as e:
        status, err = "FAILED", str(e)[:1000]
        if pg is not None:
            pg.rollback()
    finally:
        if own_duck:
            duck.close()
        if own_pg:
            pg.close()
    if log:
        _log([(started, run_id, refresh_tag, caller, query_id, q["widget"], q["question"], tier, engine,
               round(ms, 3) if ms is not None else None, len(rows), status, err)])
    return {"query_id": query_id, "tier": tier, "status": status, "exec_ms": ms, "rows": [list(map(_j, r)) for r in rows[:500]],
            "error_message": err}


def _j(v):
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    try:
        return float(v) if v is not None and not isinstance(v, (str, int, float)) else v
    except Exception:
        return str(v)


# ------------------------------------------------------------------------------------------- optimisation step
def optimize_mart(run_id=None):
    """Physical optimisation applied after each load (Airflow task 'apply_optimizations')."""
    with mart_lock():
        return _optimize_mart(run_id)


def _export_parquet(con):
    target = PARQUET_DIR / "fact_sales"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    con.execute(f"""COPY (SELECT *, CAST(sale_date_key / 10000 AS INT) AS sale_year,
                          CAST((sale_date_key / 100) % 100 AS INT) AS sale_month FROM fact_sales)
                    TO '{target}' (FORMAT parquet, PARTITION_BY (sale_year, sale_month), COMPRESSION zstd)""")


def _parquet_is_current(con) -> bool:
    try:
        n_pq = con.execute(f"SELECT count(*) FROM read_parquet('{PARQUET_DIR / 'fact_sales'}/*/*/*.parquet')").fetchone()[0]
    except Exception:
        return False
    return n_pq == con.execute("SELECT count(*) FROM fact_sales").fetchone()[0]


def _optimize_mart(run_id=None):
    con = duckdb.connect(str(DUCKDB_PATH))
    if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'fact_sales'").fetchone()[0]:
        con.close()
        return None             # mart not built yet (fresh stack, first scheduled run): nothing to optimise / query
    t0 = time.perf_counter()
    actions = []
    # 1) re-cluster the transaction fact by date then medicine -> DuckDB zone maps (min/max per row group) can skip
    con.execute("CREATE OR REPLACE TABLE fact_sales__sorted AS SELECT * FROM fact_sales "
                "ORDER BY sale_date_key, medicine_key")
    con.execute("DROP TABLE fact_sales")
    con.execute("ALTER TABLE fact_sales__sorted RENAME TO fact_sales")
    actions.append("fact_sales re-clustered ORDER BY sale_date_key, medicine_key (zone-map pruning)")
    # 2) ART indexes on natural keys used by ETL lookups / point queries
    for ix, tbl, col in [("ix_dim_medicine_id", "dim_medicine", "medicine_id"),
                         ("ix_dim_hospital_id", "dim_hospital", "hospital_id"),
                         ("ix_dim_pharmacy_id", "dim_pharmacy", "pharmacy_id"),
                         ("ix_fact_sales_txn", "fact_sales", "sales_transaction_id")]:
        con.execute(f"CREATE INDEX IF NOT EXISTS {ix} ON {tbl} ({col})")
    actions.append("ART indexes on natural keys + degenerate transaction id")
    # 3) hive-partitioned Parquet copy of the fact (sale_year / sale_month) -> partition pruning, lake-ready (gold)
    _export_parquet(con)
    actions.append("fact_sales exported to Parquet partitioned by sale_year/sale_month (zstd)")
    con.execute("CHECKPOINT")
    con.close()
    return {"actions": actions, "duration_s": round(time.perf_counter() - t0, 2)}


# ------------------------------------------------------------------------------------------- benchmark
def benchmark(run_id=None, refresh_tag=None, repeats=3, tiers=("oltp_staging", "star_schema", "star_optimized",
                                                                 "lake_parquet"), check_equivalence=True):
    with ready_mart():     # same committed snapshot for every tier
        return _benchmark(run_id, refresh_tag, repeats, tiers, check_equivalence)


def _benchmark(run_id, refresh_tag, repeats, tiers, check_equivalence):
    run_id = run_id or f"QB-{uuid.uuid4().hex[:8]}"
    duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    if "lake_parquet" in tiers and not _parquet_is_current(duck):
        _export_parquet(duck)   # a transform committed after apply_optimizations: refresh the lake copy (lock held)
    pg = psycopg2.connect(PG_DSN)
    with pg.cursor() as cur:
        cur.execute("SET statement_timeout = '60s'")
    pg.commit()
    rows, results = [], {}
    for q in QUERIES:
        for tier in [t for t in tiers if t in q["sql"]]:
            for i in range(repeats):
                started = dt.datetime.now(dt.timezone.utc)
                try:
                    res, ms = _execute(ENGINE[tier], q["sql"][tier], duck, pg)
                    status, err = "SUCCESS", None
                    if i == 0:
                        results[(q["id"], tier)] = res
                except Exception as e:
                    res, ms, status, err = [], None, "FAILED", str(e)[:1000]
                    pg.rollback()
                rows.append((started, run_id, refresh_tag, "benchmark", q["id"], q["widget"], q["question"], tier,
                             ENGINE[tier], round(ms, 3) if ms is not None else None, len(res), status, err))
    _log(rows)
    duck.close()
    pg.close()
    equivalence = {}
    if check_equivalence:
        for q in QUERIES:
            base = results.get((q["id"], "star_schema"))
            for tier in [t for t in tiers if t in q["sql"]]:
                other = results.get((q["id"], tier))
                equivalence[(q["id"], tier)] = _same(base, other)
    return {"run_id": run_id, "executions": len(rows), "equivalence": equivalence}


def _same(a, b, tol=0.02):
    if a is None or b is None or len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        for x, y in zip(ra, rb):
            if isinstance(x, (int, float)) or hasattr(x, "__float__"):
                try:
                    if abs(float(x) - float(y)) > tol * max(1, abs(float(x))) and abs(float(x) - float(y)) > 0.011:
                        return False
                    continue
                except (TypeError, ValueError):
                    pass
            if str(x) != str(y):
                return False
    return True


def dashboard_refresh(run_id=None, refresh_tag=None):
    """Simulates the BI layer refreshing every widget once (optimised tier) - counted in 'daily active queries'."""
    with mart_lock():
        return _dashboard_refresh(run_id, refresh_tag)


def _dashboard_refresh(run_id=None, refresh_tag=None):
    duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    out = [run_catalog_query(q["id"], "star_optimized", "dashboard_refresh", run_id, refresh_tag, duck=duck)
           for q in QUERIES]
    duck.close()
    return {r["query_id"]: round(r["exec_ms"] or -1, 2) for r in out}


def adhoc_simulation(run_id=None, refresh_tag=None):
    with mart_lock():
        return _adhoc_simulation(run_id, refresh_tag)


def _adhoc_simulation(run_id=None, refresh_tag=None):
    duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    pg = psycopg2.connect(PG_DSN)
    rows = []
    for qid, engine, sql in ADHOC:
        started = dt.datetime.now(dt.timezone.utc)
        try:
            res, ms = _execute(engine, sql, duck, pg)
            status, err = "SUCCESS", None
        except Exception as e:
            res, ms, status, err = [], None, "FAILED", str(e).split("\n")[0][:500]
            pg.rollback()
        rows.append((started, run_id, refresh_tag, "adhoc_analyst", qid, "Ad-hoc analyst query", sql, "adhoc",
                     engine, round(ms, 3) if ms is not None else None, len(res), status, err))
    _log(rows)
    duck.close()
    pg.close()
    return {r[4]: r[11] for r in rows}
