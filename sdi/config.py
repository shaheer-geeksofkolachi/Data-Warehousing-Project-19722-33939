"""Central configuration. Every path / connection string can be overridden by environment variables
so the same code runs inside Docker (Airflow containers) and on a laptop."""
import os
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("SDI_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = Path(os.getenv("SDI_DATA_DIR", PROJECT_ROOT / "data"))

SOURCE_DIR = DATA_DIR / "source"          # original 7 provided files (base load)
GENERATED_DIR = DATA_DIR / "generated"    # output of the oversampling generator, one folder per refresh
LANDING_DIR = DATA_DIR / "landing"        # watched by the file-watcher (batch ingestion entry point)
ARCHIVE_DIR = DATA_DIR / "archive"        # files moved here after successful ingestion
QUARANTINE_DIR = DATA_DIR / "quarantine"  # files that failed ingestion entirely
STREAM_QUEUE_DIR = DATA_DIR / "stream_queue"  # sales increments waiting to be streamed through the API
WORK_DIR = DATA_DIR / "work"              # task hand-off files between Airflow tasks
MART_DIR = DATA_DIR / "mart"
DUCKDB_PATH = Path(os.getenv("SDI_DUCKDB_PATH", MART_DIR / "sdi_mart.duckdb"))
PARQUET_DIR = MART_DIR / "parquet"

PG_DSN = os.getenv("SDI_PG_DSN", "host=localhost port=5432 dbname=pharma_sdi user=postgres password=postgres")
PG_DATA_DIR = os.getenv("SDI_PG_DATA_DIR", "")  # optional: postgres data dir, for os.path.getsize footprint

INGEST_API_URL = os.getenv("SDI_INGEST_API_URL", "http://localhost:8000")
AIRFLOW_API_URL = os.getenv("SDI_AIRFLOW_API_URL", "")  # e.g. http://airflow-apiserver:8080 (optional trigger)
AIRFLOW_USER = os.getenv("SDI_AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("SDI_AIRFLOW_PASSWORD", "airflow")

# Streaming simulation defaults
STREAM_EVENTS_PER_SEC = int(os.getenv("SDI_STREAM_EPS", "400"))
STREAM_MICROBATCH = int(os.getenv("SDI_STREAM_MICROBATCH", "50"))

# Refresh calendar: base load = provided 2023-2025 history; two oversampled refreshes
REFRESHES = {
    "R1": {"month": "2026-01", "label": "Refresh 1 - January 2026"},
    "R2": {"month": "2026-02", "label": "Refresh 2 - February 2026"},
}

for _d in (SOURCE_DIR, GENERATED_DIR, LANDING_DIR, ARCHIVE_DIR, QUARANTINE_DIR, STREAM_QUEUE_DIR, WORK_DIR, MART_DIR):
    _d.mkdir(parents=True, exist_ok=True)
