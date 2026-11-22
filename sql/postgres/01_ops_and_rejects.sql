-- =====================================================================
-- Operational (ops) schema: every pipeline layer writes its metrics here.
-- Grafana reads ONLY these tables -> the 4 Data-Engineering dashboards.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS rejects;
CREATE SCHEMA IF NOT EXISTS ops;

-- File registry: the watcher registers every new landing file here (via the ingestion API).
CREATE TABLE IF NOT EXISTS ops.file_registry (
    file_id         BIGSERIAL PRIMARY KEY,
    file_name       TEXT NOT NULL,
    file_path       TEXT NOT NULL UNIQUE,
    file_size_mb    NUMERIC(12,3),
    refresh_tag     TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | PROCESSING | LOADED | FAILED
    processed_at    TIMESTAMPTZ,
    detected_by     TEXT                               -- watcher | api | dag
);

-- One row per source sheet/file loaded by the batch ingestion DAG  -> Batch Ingestion dashboard
CREATE TABLE IF NOT EXISTS ops.batch_load_log (
    load_id         BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    dag_run_id      TEXT,
    refresh_tag     TEXT,
    source_name     TEXT NOT NULL,
    domain          TEXT,
    file_name       TEXT,
    file_size_mb    NUMERIC(12,3),
    start_ts        TIMESTAMPTZ NOT NULL,
    end_ts          TIMESTAMPTZ,
    duration_s      NUMERIC(12,3),
    read_s          NUMERIC(12,3),
    validate_s      NUMERIC(12,3),
    insert_s        NUMERIC(12,3),
    rows_read       BIGINT,
    rows_loaded     BIGINT,
    rows_rejected   BIGINT,
    rows_duplicate  BIGINT,
    rows_per_sec    NUMERIC(14,1),
    status          TEXT NOT NULL,          -- SUCCESS | FAILED
    error_message   TEXT
);

-- Streaming: one row per streaming session (one refresh) -> Streaming dashboard
CREATE TABLE IF NOT EXISTS ops.stream_session (
    session_id        TEXT PRIMARY KEY,
    refresh_tag       TEXT,
    source_name       TEXT,
    target_eps        INT,
    start_ts          TIMESTAMPTZ,
    end_ts            TIMESTAMPTZ,
    duration_s        NUMERIC(12,3),
    events_sent       BIGINT,
    events_ingested   BIGINT,
    events_malformed  BIGINT,
    events_duplicate  BIGINT,
    throughput_eps    NUMERIC(12,2),
    avg_latency_ms    NUMERIC(12,2),
    p95_latency_ms    NUMERIC(12,2),
    status            TEXT
);

-- Streaming: per-second buckets (throughput / latency over time)
CREATE TABLE IF NOT EXISTS ops.stream_metrics (
    session_id        TEXT NOT NULL,
    bucket_ts         TIMESTAMPTZ NOT NULL,     -- truncated to the second
    events_ingested   INT NOT NULL DEFAULT 0,
    events_malformed  INT NOT NULL DEFAULT 0,
    events_duplicate  INT NOT NULL DEFAULT 0,
    latency_sum_ms    DOUBLE PRECISION NOT NULL DEFAULT 0,
    latency_max_ms    DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, bucket_ts)
);

-- Individual event latencies (sampled 100%) for percentile panels
CREATE TABLE IF NOT EXISTS ops.stream_event_latency (
    session_id      TEXT NOT NULL,
    event_id        TEXT,
    created_ts      TIMESTAMPTZ NOT NULL,   -- when the producer created the event
    committed_ts    TIMESTAMPTZ NOT NULL,   -- when the row was committed to staging
    latency_ms      DOUBLE PRECISION NOT NULL
);

-- Storage snapshots -> Storage dashboard
CREATE TABLE IF NOT EXISTS ops.storage_snapshot (
    snapshot_id     BIGSERIAL PRIMARY KEY,
    snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    refresh_tag     TEXT,
    layer           TEXT NOT NULL,          -- staging | rejects | mart
    table_name      TEXT NOT NULL,
    domain          TEXT,
    row_count       BIGINT,
    size_mb         NUMERIC(14,3),
    min_date        DATE,
    max_date        DATE,
    span_days       INT,
    distinct_months INT
);
CREATE TABLE IF NOT EXISTS ops.storage_footprint (
    snapshot_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    refresh_tag         TEXT,
    pg_database_mb      NUMERIC(14,3),
    pg_datadir_mb       NUMERIC(14,3),
    duckdb_file_mb      NUMERIC(14,3),
    parquet_dir_mb      NUMERIC(14,3)
);
CREATE TABLE IF NOT EXISTS ops.date_coverage (
    snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    refresh_tag     TEXT,
    source_name     TEXT NOT NULL,
    year_month      TEXT NOT NULL,
    row_count       BIGINT
);

-- Query layer -> Query dashboard
CREATE TABLE IF NOT EXISTS ops.query_log (
    query_log_id    BIGSERIAL PRIMARY KEY,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id          TEXT,
    refresh_tag     TEXT,
    caller          TEXT,                   -- benchmark | dashboard_refresh | api
    query_id        TEXT NOT NULL,
    widget_name     TEXT,
    business_question TEXT,
    tier            TEXT NOT NULL,          -- oltp_staging | star_schema | star_optimized
    engine          TEXT,                   -- postgres | duckdb
    exec_ms         NUMERIC(14,3),
    rows_returned   BIGINT,
    status          TEXT NOT NULL,          -- SUCCESS | FAILED
    error_message   TEXT
);

-- Transformation layer: watermarks + per-run statistics
CREATE TABLE IF NOT EXISTS ops.transform_watermark (
    staging_table   TEXT PRIMARY KEY,
    last_stg_id     BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ops.transform_log (
    transform_id    BIGSERIAL PRIMARY KEY,
    run_id          TEXT,
    refresh_tag     TEXT,
    step            TEXT,
    target_table    TEXT,
    rows_in         BIGINT,
    rows_inserted   BIGINT,
    rows_updated    BIGINT,
    duration_s      NUMERIC(12,3),
    status          TEXT,
    details         TEXT,
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Oversampling generator statistics
CREATE TABLE IF NOT EXISTS ops.generator_log (
    refresh_tag     TEXT,
    source_name     TEXT,
    target_month    TEXT,
    rows_generated  BIGINT,
    dirty_rows      BIGINT,
    output_file     TEXT,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Rejected rows from batch validation and stream parsing
CREATE TABLE IF NOT EXISTS rejects.rejected_rows (
    reject_id       BIGSERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL,
    batch_id        TEXT,
    ingest_mode     TEXT,                   -- batch | stream
    reason_code     TEXT NOT NULL,
    reason_detail   TEXT,
    record          JSONB,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_query_log_ts ON ops.query_log (executed_at);
CREATE INDEX IF NOT EXISTS ix_batch_log_ts ON ops.batch_load_log (start_ts);
CREATE INDEX IF NOT EXISTS ix_stream_lat_session ON ops.stream_event_latency (session_id);
CREATE INDEX IF NOT EXISTS ix_rejects_source ON rejects.rejected_rows (source_name, reason_code);

-- Airflow task outcomes (written by the DAG callbacks) -> pipeline health panels
CREATE TABLE IF NOT EXISTS ops.pipeline_task_log (
    logged_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id          TEXT,
    task_id         TEXT,
    run_id          TEXT,
    try_number      INT,
    state           TEXT,
    start_ts        TIMESTAMPTZ,
    end_ts          TIMESTAMPTZ,
    duration_s      NUMERIC(12,3),
    refresh_tag     TEXT
);

-- Mart consistency flag: LOADING while a transform is between extract and reconcile, READY once the watermark is
-- committed. The query layer only benchmarks a READY mart, so every tier sees the same snapshot.
CREATE TABLE IF NOT EXISTS ops.mart_state (
    id          INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    status      TEXT NOT NULL,
    run_id      TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO ops.mart_state (id, status, run_id) VALUES (1, 'READY', 'init') ON CONFLICT (id) DO NOTHING;