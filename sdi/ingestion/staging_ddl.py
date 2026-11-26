"""Creates the Postgres staging tables (ODS) from the source registry.

Staging keeps the source structure 1:1 (snake_case columns, typed), plus lineage metadata.
Rows are append-only: new data is ADDED to history, never truncated."""
from pathlib import Path
from ..sources import SOURCES, snake
from ..db import pg_conn
from ..config import PROJECT_ROOT

PG_TYPES = {"text": "TEXT", "bigint": "BIGINT", "double": "DOUBLE PRECISION", "date": "DATE", "timestamp": "TIMESTAMPTZ"}

META_COLUMNS = """    _stg_id            BIGSERIAL,
    _batch_id          TEXT,
    _source_file       TEXT,
    _refresh_tag       TEXT,
    _ingest_mode       TEXT,
    _event_created_at  TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()"""


def staging_ddl() -> str:
    parts = []
    for src in sorted(SOURCES.values(), key=lambda s: s.load_order):
        cols = ",\n".join(f"    {snake(c):40s} {PG_TYPES[t]}" for c, t in src.columns)
        pk = ", ".join(snake(c) for c in src.pk)
        parts.append(
            f"-- {src.key}: {src.domain}\nCREATE TABLE IF NOT EXISTS staging.{src.table} (\n{cols},\n{META_COLUMNS},\n"
            f"    CONSTRAINT pk_{src.table} PRIMARY KEY ({pk})\n);\n"
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{src.table}_stg_id ON staging.{src.table} (_stg_id);\n")
    return "CREATE SCHEMA IF NOT EXISTS staging;\n\n" + "\n".join(parts)


def ensure_superset_db(name="superset"):
    """Superset keeps its metadata (dashboards, charts, users) in its own Postgres database."""
    import re
    import psycopg2
    from ..config import PG_DSN
    dsn = re.sub(r"dbname=\S+", "dbname=postgres", PG_DSN)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {name}")
    conn.close()


def init_postgres():
    """Idempotent: creates ops/rejects/staging schemas and tables (+ the Superset metadata database)."""
    try:
        ensure_superset_db()
    except Exception as e:      # never block the pipeline on the BI metadata DB
        print(f"[sdi] could not ensure superset database: {e}")
    ops_sql = (PROJECT_ROOT / "sql/postgres/01_ops_and_rejects.sql").read_text()
    ddl = staging_ddl()
    Path(PROJECT_ROOT / "sql/postgres/02_staging_tables.sql").write_text(
        "-- AUTO-GENERATED from sdi/sources.py by sdi.ingestion.staging_ddl\n" + ddl)
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(ops_sql)
        cur.execute(ddl)
        for src in SOURCES.values():
            cur.execute("INSERT INTO ops.transform_watermark (staging_table) VALUES (%s) ON CONFLICT DO NOTHING",
                        (src.table,))
            # committed snapshot: staging rows already transformed into the mart (<= last committed watermark).
            # The OLTP query tier reads these views so it always describes the same data as the mart.
            cur.execute(f"""CREATE OR REPLACE VIEW staging.c_{src.table} AS SELECT * FROM staging.{src.table}
                            WHERE _stg_id <= (SELECT last_stg_id FROM ops.transform_watermark
                                              WHERE staging_table = '{src.table}')""")


if __name__ == "__main__":
    init_postgres()
    print("Postgres schemas initialised")
