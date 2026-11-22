"""DANGER: wipes staging/ops/rejects schemas, the DuckDB mart and all pipeline folders, then re-creates schemas.
Use before a clean demo run:  python scripts/reset_environment.py --yes"""
import sys
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sdi.config import (LANDING_DIR, ARCHIVE_DIR, QUARANTINE_DIR, STREAM_QUEUE_DIR, GENERATED_DIR, WORK_DIR,
                        MART_DIR, DUCKDB_PATH)
from sdi.db import pg_exec
from sdi.ingestion.staging_ddl import init_postgres

if "--yes" not in sys.argv:
    sys.exit("refusing to reset without --yes")
pg_exec("DROP SCHEMA IF EXISTS staging CASCADE; DROP SCHEMA IF EXISTS ops CASCADE; DROP SCHEMA IF EXISTS rejects CASCADE;")
for d in (LANDING_DIR, ARCHIVE_DIR, QUARANTINE_DIR, STREAM_QUEUE_DIR, GENERATED_DIR, WORK_DIR, MART_DIR):
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)
for f in Path(DUCKDB_PATH).parent.glob(Path(DUCKDB_PATH).name + "*"):
    f.unlink()
init_postgres()
print("environment reset: schemas re-created, folders emptied")
