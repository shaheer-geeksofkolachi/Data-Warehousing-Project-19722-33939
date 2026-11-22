"""Storage & staging layer metrics (-> ops.storage_* -> Grafana 'Storage' dashboard).

  Table row count      SELECT COUNT(*) per staging / rejects / mart table (dimension + fact + aggregate)
  Storage footprint    pg_total_relation_size per table, pg_database_size, os.path.getsize of the DuckDB file,
                       Parquet directory size (and the Postgres data directory when it is mounted)
  Date coverage span   MIN/MAX business date per source + distinct year-month partitions (2023-01 .. latest)
"""
import os
from pathlib import Path

import duckdb

from ..config import DUCKDB_PATH, PARQUET_DIR, PG_DATA_DIR
from ..db import pg_conn, pg_query, mart_lock
from ..sources import SOURCES, snake


def _dir_mb(p: Path):
    if not p or not Path(p).exists():
        return None
    return round(sum(f.stat().st_size for f in Path(p).rglob("*") if f.is_file()) / 1e6, 3)


def collect(refresh_tag=None):
    with mart_lock():          # DuckDB is opened read-only: wait for any writer to finish first
        return _collect(refresh_tag)


def _collect(refresh_tag=None):
    rows, cov = [], []
    # ---- staging (Postgres)
    for src in SOURCES.values():
        dcol = snake(src.coverage_date)
        r = pg_query(f"""SELECT count(*), pg_total_relation_size('staging.{src.table}') / 1e6,
                                min({dcol})::date, max({dcol})::date,
                                count(DISTINCT to_char({dcol}, 'YYYY-MM'))
                         FROM staging.{src.table}""")[0]
        span = (r[3] - r[2]).days if r[2] and r[3] else None
        rows.append(("staging", src.table, src.domain, r[0], r[1], r[2], r[3], span, r[4]))
        for ym, n in pg_query(f"SELECT to_char({dcol}, 'YYYY-MM'), count(*) FROM staging.{src.table} GROUP BY 1"):
            cov.append((refresh_tag, src.key, ym, n))
    r = pg_query("SELECT count(*), pg_total_relation_size('rejects.rejected_rows')/1e6, min(rejected_at)::date, "
                 "max(rejected_at)::date FROM rejects.rejected_rows")[0]
    rows.append(("rejects", "rejected_rows", "Rejected records", r[0], r[1], r[2], r[3], None, None))
    # ---- mart (DuckDB)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    block = con.execute("SELECT block_size FROM pragma_database_size()").fetchone()[0]
    tables = [t for (t,) in con.execute("SELECT table_name FROM duckdb_tables() ORDER BY 1").fetchall()]
    date_col = {"fact_sales": "sale_date_key", "fact_hospital_demand": "demand_month_key"}
    total_db_mb = os.path.getsize(DUCKDB_PATH) / 1e6
    for t in tables:
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        # physical size = distinct storage blocks used by the table's column segments x block size
        mb = con.execute(f"SELECT count(DISTINCT block_id) * {block} / 1e6 FROM pragma_storage_info('{t}') "
                         f"WHERE block_id >= 0").fetchone()[0]
        lo = hi = span = months = None
        if t in date_col:
            lo, hi, months = con.execute(
                f"SELECT min(d.full_date), max(d.full_date), count(DISTINCT d.year_month) FROM {t} f "
                f"JOIN dim_date d ON d.date_key = f.{date_col[t]}").fetchone()
            span = (hi - lo).days if lo and hi else None
        kind = "fact" if t.startswith("fact") else "aggregate" if t.startswith("agg") else "dimension"
        rows.append(("mart", t, kind, n, round(mb, 3), lo, hi, span, months))
    con.close()
    fp = (pg_query("SELECT pg_database_size(current_database())/1e6")[0][0], _dir_mb(PG_DATA_DIR) if PG_DATA_DIR else None,
          round(total_db_mb, 3), _dir_mb(PARQUET_DIR))
    with pg_conn() as c, c.cursor() as cur:
        cur.executemany("""INSERT INTO ops.storage_snapshot (refresh_tag, layer, table_name, domain, row_count, size_mb,
                           min_date, max_date, span_days, distinct_months) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [(refresh_tag, *r) for r in rows])
        cur.execute("INSERT INTO ops.storage_footprint (refresh_tag, pg_database_mb, pg_datadir_mb, duckdb_file_mb, "
                    "parquet_dir_mb) VALUES (%s,%s,%s,%s,%s)", (refresh_tag, *fp))
        cur.executemany("INSERT INTO ops.date_coverage (refresh_tag, source_name, year_month, row_count) "
                        "VALUES (%s,%s,%s,%s)", cov)
    return {"tables": len(rows), "pg_database_mb": float(fp[0]), "duckdb_file_mb": fp[2], "parquet_dir_mb": fp[3]}


if __name__ == "__main__":
    print(collect())
