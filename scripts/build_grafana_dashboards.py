"""Generates the Grafana dashboard JSON files (grafana/dashboards/*.json) for the Data-Engineering observability layer.
All panels query the Postgres `ops` schema. Re-run after editing:  python scripts/build_grafana_dashboards.py"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "grafana" / "dashboards"
DS = {"type": "grafana-postgresql-datasource", "uid": "sdi-postgres"}
REFRESH_FILTER = "refresh_tag IN (${refresh:sqlstring})"

_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


def target(sql, fmt="table", ref="A"):
    return {"refId": ref, "datasource": DS, "rawSql": sql.strip(), "format": fmt, "rawQuery": True, "editorMode": "code"}


def panel(title, ptype, sql, x, y, w, h, fmt="table", desc="", unit=None, options=None, field=None, overrides=None,
          transformations=None, extra_targets=None):
    p = {"id": nid(), "title": title, "type": ptype, "datasource": DS, "description": desc,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [target(sql, fmt)] + (extra_targets or []),
         "options": options or {}, "fieldConfig": {"defaults": field or {}, "overrides": overrides or []}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if transformations:
        p["transformations"] = transformations
    return p


def stat(title, sql, x, y, w=4, h=4, unit=None, desc="", color="blue", decimals=None, thresholds=None):
    f = {"color": {"mode": "fixed", "fixedColor": color}}
    if decimals is not None:
        f["decimals"] = decimals
    if thresholds:
        f["color"] = {"mode": "thresholds"}
        f["thresholds"] = {"mode": "absolute", "steps": thresholds}
    return panel(title, "stat", sql, x, y, w, h, unit=unit, desc=desc, field=f,
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "background", "graphMode": "none", "textMode": "value",
                          "justifyMode": "center"})


def text(content, x, y, w, h):
    return {"id": nid(), "type": "text", "title": "", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": content}}


def row(title, y):
    return {"id": nid(), "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
            "panels": []}


def barchart(title, sql, x, y, w, h, horizontal=False, unit=None, desc="", stacked=False, log=False, xfield=None):
    opts = {"orientation": "horizontal" if horizontal else "vertical", "showValue": "auto", "groupWidth": 0.75,
            "barWidth": 0.9, "stacking": "normal" if stacked else "none", "legend": {"displayMode": "list",
                                                                                      "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi"}, "xTickLabelRotation": 0 if horizontal else -30}
    if xfield:
        opts["xField"] = xfield
    field = {"color": {"mode": "palette-classic"}, "custom": {"fillOpacity": 85, "lineWidth": 0}}
    if log:
        field["custom"]["scaleDistribution"] = {"type": "log", "log": 10}
    return panel(title, "barchart", sql, x, y, w, h, unit=unit, desc=desc, options=opts, field=field)


def pie(title, sql, x, y, w, h, unit=None, desc="", donut=True):
    return panel(title, "piechart", sql, x, y, w, h, unit=unit, desc=desc,
                 options={"pieType": "donut" if donut else "pie", "reduceOptions": {"calcs": ["lastNotNull"],
                          "fields": "", "values": True}, "legend": {"displayMode": "table", "placement": "right",
                          "showLegend": True, "values": ["value", "percent"]}, "displayLabels": ["percent"]},
                 field={"color": {"mode": "palette-classic"}})


def table(title, sql, x, y, w, h, desc="", overrides=None):
    return panel(title, "table", sql, x, y, w, h, desc=desc, options={"showHeader": True, "cellHeight": "sm",
                 "footer": {"show": False}}, field={"custom": {"align": "auto", "cellOptions": {"type": "auto"}}},
                 overrides=overrides)


def timeseries(title, sql, x, y, w, h, unit=None, desc="", bars=False, extra_targets=None):
    return panel(title, "timeseries", sql, x, y, w, h, fmt="time_series", unit=unit, desc=desc,
                 options={"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                          "tooltip": {"mode": "multi"}},
                 field={"color": {"mode": "palette-classic"},
                        "custom": {"drawStyle": "bars" if bars else "line", "lineWidth": 2, "fillOpacity": 25 if bars else 10,
                                   "pointSize": 4, "showPoints": "auto", "spanNulls": False, "lineInterpolation": "linear"}},
                 extra_targets=extra_targets)


def dashboard(uid, title, panels, desc, refresh_var=True, time_from="now-24h"):
    templating = []
    if refresh_var:
        templating.append({"name": "refresh", "label": "Refresh", "type": "query", "datasource": DS,
                           "query": "SELECT DISTINCT refresh_tag FROM ops.batch_load_log WHERE refresh_tag IS NOT NULL ORDER BY 1",
                           "definition": "SELECT DISTINCT refresh_tag FROM ops.batch_load_log ORDER BY 1",
                           "multi": True, "includeAll": True, "current": {"selected": True, "text": ["All"],
                                                                          "value": ["$__all"]},
                           "refresh": 2, "sort": 1})
    return {"uid": uid, "title": title, "description": desc, "tags": ["sdi", "data-engineering", "phase1"],
            "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True, "refresh": "30s",
            "time": {"from": time_from, "to": "now"}, "templating": {"list": templating}, "panels": panels,
            "annotations": {"list": [{"name": "Refresh cycles", "datasource": DS, "enable": True, "iconColor": "orange",
                                      "target": target("SELECT min(start_ts) AS time, 'Refresh ' || refresh_tag AS text "
                                                       "FROM ops.batch_load_log GROUP BY refresh_tag", "table")}]}}


# ============================================================================================ 1. BATCH INGESTION
def batch_dashboard():
    _id[0] = 0
    p = [text("### Batch ingestion — Excel landing files → Postgres staging (ODS)\n"
              "The **file-watcher** detects each new Excel file in `landing/`, the **ingestion API** registers it and the "
              "**Airflow DAG `sdi_01_batch_ingestion`** reads, validates and bulk-loads every sheet. "
              "Use the *Refresh* selector to compare the BASE historical load with refresh R1/R2.", 0, 0, 24, 3)]
    y = 3
    p += [stat("Files / sheets loaded", f"SELECT count(*) FROM ops.batch_load_log WHERE {REFRESH_FILTER}", 0, y, 4),
          stat("Rows loaded", f"SELECT sum(rows_loaded) FROM ops.batch_load_log WHERE {REFRESH_FILTER}", 4, y, 4,
               unit="short", color="green"),
          stat("Batch success rate", f"SELECT 100.0 * count(*) FILTER (WHERE status='SUCCESS') / nullif(count(*),0) "
                                     f"FROM ops.batch_load_log WHERE {REFRESH_FILTER}", 8, y, 4, unit="percent",
               decimals=1, thresholds=[{"color": "red", "value": None}, {"color": "orange", "value": 95},
                                       {"color": "green", "value": 99.9}],
               desc="Successful runs / total attempted runs x 100 (a run = one sheet load)"),
          stat("Rows rejected by validation", f"SELECT sum(rows_rejected) FROM ops.batch_load_log WHERE {REFRESH_FILTER}",
               12, y, 4, color="orange"),
          stat("Duplicates skipped (idempotent)", f"SELECT sum(rows_duplicate) FROM ops.batch_load_log WHERE {REFRESH_FILTER}",
               16, y, 4, color="purple", desc="Rows whose primary key already existed in staging (re-delivered master rows)"),
          stat("Total load time", f"SELECT sum(duration_s) FROM ops.batch_load_log WHERE {REFRESH_FILTER}", 20, y, 4,
               unit="s", decimals=1, color="blue")]
    y += 4
    p += [barchart("Batch load duration per Excel source (seconds)",
                   f"""SELECT source_name || ' [' || refresh_tag || ']' AS source, round(duration_s, 2) AS "duration (s)"
                       FROM ops.batch_load_log WHERE {REFRESH_FILTER} ORDER BY duration_s DESC""",
                   0, y, 12, 11, horizontal=True, unit="s",
                   desc="Batch Load Duration = end_ts - start_ts per sheet (read + validate + insert). The 500k-row "
                        "sales sheet dominates the BASE load."),
          barchart("Time breakdown per source (BASE load): read vs validate vs insert",
                   f"""SELECT source_name AS source, round(read_s,2) AS "read Excel", round(validate_s,2) AS "validate",
                              round(insert_s,2) AS "insert (COPY)"
                       FROM ops.batch_load_log WHERE refresh_tag = 'BASE' ORDER BY duration_s DESC""",
                   12, y, 12, 11, horizontal=True, unit="s", stacked=True,
                   desc="Where the time goes: parsing Excel is the most expensive step; COPY into Postgres is cheap.")]
    y += 11
    p += [barchart("Rows processed per batch (log scale) by refresh",
                   f"""SELECT source_name AS source,
                              sum(rows_read) FILTER (WHERE refresh_tag='BASE') AS "BASE",
                              sum(rows_read) FILTER (WHERE refresh_tag='R1') AS "R1",
                              sum(rows_read) FILTER (WHERE refresh_tag='R2') AS "R2"
                       FROM ops.batch_load_log GROUP BY 1 ORDER BY 1""",
                   0, y, 12, 9, log=True, desc="Rows processed per batch = len(df) after reading each sheet."),
          barchart("Rejected rows by validation rule",
                   f"""SELECT reason_code AS rule, count(*) FILTER (WHERE ingest_mode='batch') AS "batch files",
                              count(*) FILTER (WHERE ingest_mode='stream') AS "stream events"
                       FROM rejects.rejected_rows GROUP BY 1 ORDER BY 2 DESC""",
                   12, y, 12, 9, desc="Rules: non-null keys, MedicineID in master, price > 0, valid dates, "
                                      "net = gross - discount, sensor range, duplicate keys.")]
    y += 9
    p += [table("Batch load log (latest first)",
                f"""SELECT start_ts AS "start", refresh_tag AS refresh, file_name AS "file [sheet]", rows_read, rows_loaded,
                           rows_rejected, rows_duplicate, round(duration_s,2) AS duration_s, rows_per_sec, status, dag_run_id
                    FROM ops.batch_load_log WHERE {REFRESH_FILTER} ORDER BY load_id DESC""", 0, y, 16, 9),
          table("File registry (file-watcher → API → DAG)",
                """SELECT file_name, refresh_tag, detected_by, status, detected_at, processed_at
                   FROM ops.file_registry ORDER BY file_id DESC""", 16, y, 8, 9)]
    return dashboard("sdi-batch-ingestion", "SDI · 1 · Batch Ingestion", p,
                     "Batch Load Duration, Rows Processed per Batch, Batch Success Rate", time_from="now-7d")


# ============================================================================================ 2. STREAMING
def stream_dashboard():
    _id[0] = 0
    p = [text("### Streaming ingestion — pharmacy & e-commerce sales events → ingestion API → Postgres staging\n"
              "The producer replays each refresh's sales as a live stream (micro-batches of 50 events, target 400 events/s). "
              "**Latency** = DB commit time − event creation time. Malformed events are counted in the API's validation "
              "try/except and written to `rejects.rejected_rows`. *Set the time range to cover the refresh runs.*",
              0, 0, 24, 3)]
    y = 3
    p += [stat("Events ingested", f"SELECT sum(events_ingested) FROM ops.stream_session WHERE {REFRESH_FILTER}", 0, y, 4,
               color="green"),
          stat("Throughput (events/sec)", f"SELECT sum(events_ingested)/nullif(sum(duration_s),0) FROM ops.stream_session "
                                         f"WHERE {REFRESH_FILTER}", 4, y, 4, decimals=1, color="blue",
               desc="Total records ingested / total stream duration (seconds)"),
          stat("Avg end-to-end latency", f"SELECT sum(avg_latency_ms*events_ingested)/nullif(sum(events_ingested),0) "
                                        f"FROM ops.stream_session WHERE {REFRESH_FILTER}", 8, y, 4, unit="ms",
               decimals=0, thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 500},
                                       {"color": "red", "value": 2000}]),
          stat("p95 latency", f"SELECT max(p95_latency_ms) FROM ops.stream_session WHERE {REFRESH_FILTER}", 12, y, 4,
               unit="ms", decimals=0, color="purple"),
          stat("Malformed / dropped events", f"SELECT sum(events_malformed) FROM ops.stream_session WHERE {REFRESH_FILTER}",
               16, y, 4, color="orange", desc="Rows rejected for schema/type/null-key/business-rule violations"),
          stat("Duplicate events rejected", f"SELECT sum(events_duplicate) FROM ops.stream_session WHERE {REFRESH_FILTER}",
               20, y, 4, color="red", desc="Events whose primary key was already committed (exactly-once protection)")]
    y += 4
    p += [timeseries("Stream throughput (events/sec) during the simulation",
                     """SELECT bucket_ts AS time, s.refresh_tag AS metric, m.events_ingested AS value
                        FROM ops.stream_metrics m JOIN ops.stream_session s USING (session_id)
                        WHERE $__timeFilter(bucket_ts) ORDER BY 1""", 0, y, 16, 10, unit="short",
                     desc="Recommended chart: events committed per second. A flat line near the target rate = stable pipeline."),
          barchart("Per-session summary", f"""SELECT refresh_tag AS session, events_ingested AS ingested,
                                                    events_malformed AS malformed, events_duplicate AS duplicate
                                             FROM ops.stream_session WHERE {REFRESH_FILTER} ORDER BY 1""",
                   16, y, 8, 10, log=True)]
    y += 10
    p += [timeseries("End-to-end latency per second (avg and max, ms)",
                     """SELECT bucket_ts AS time, 'avg ' || s.refresh_tag AS metric,
                               m.latency_sum_ms / nullif(m.events_ingested,0) AS value
                        FROM ops.stream_metrics m JOIN ops.stream_session s USING (session_id)
                        WHERE $__timeFilter(bucket_ts) AND m.events_ingested > 0 ORDER BY 1""",
                     0, y, 12, 9, unit="ms",
                     extra_targets=[target("""SELECT bucket_ts AS time, 'max ' || s.refresh_tag AS metric,
                                              m.latency_max_ms AS value FROM ops.stream_metrics m
                                              JOIN ops.stream_session s USING (session_id)
                                              WHERE $__timeFilter(bucket_ts) AND m.events_ingested > 0 ORDER BY 1""",
                                           "time_series", "B")]),
          panel("Latency distribution (all events)", "histogram",
                "SELECT latency_ms FROM ops.stream_event_latency l JOIN ops.stream_session s USING (session_id) "
                f"WHERE s.{REFRESH_FILTER}", 12, y, 12, 9, unit="ms",
                options={"bucketCount": 40, "legend": {"showLegend": False}},
                field={"color": {"mode": "fixed", "fixedColor": "blue"}})]
    y += 9
    p += [timeseries("Malformed & duplicate events per second",
                     """SELECT bucket_ts AS time, 'malformed' AS metric, events_malformed AS value FROM ops.stream_metrics
                        WHERE $__timeFilter(bucket_ts) AND events_malformed > 0
                        UNION ALL SELECT bucket_ts, 'duplicate', events_duplicate FROM ops.stream_metrics
                        WHERE $__timeFilter(bucket_ts) AND events_duplicate > 0 ORDER BY 1""", 0, y, 12, 8, bars=True),
          table("Streaming sessions", f"""SELECT session_id, refresh_tag, start_ts, round(duration_s,1) AS duration_s,
                     events_sent, events_ingested, events_malformed, events_duplicate, round(throughput_eps,1) AS eps,
                     round(avg_latency_ms,1) AS avg_ms, round(p95_latency_ms,1) AS p95_ms
                     FROM ops.stream_session WHERE {REFRESH_FILTER} ORDER BY start_ts DESC""", 12, y, 12, 8)]
    return dashboard("sdi-stream-ingestion", "SDI · 2 · Streaming Ingestion", p,
                     "Stream Throughput, End-to-End Latency, Malformed / Dropped Record Count", time_from="now-24h")


# ============================================================================================ 3. STORAGE
LATEST = "snapshot_ts = (SELECT max(snapshot_ts) FROM ops.storage_snapshot)"


def storage_dashboard():
    _id[0] = 0
    p = [text("### Storage & staging — Postgres ODS (row store) and DuckDB SDI mart (column store)\n"
              "Snapshots are taken by `sdi_05_storage_metrics` after every refresh: row counts (`SELECT COUNT(*)`), "
              "physical size (Postgres `pg_total_relation_size`, DuckDB storage blocks, `os.path.getsize` of the files) "
              "and date coverage (min/max date and distinct year-month partitions per source).", 0, 0, 24, 3)]
    y = 3
    p += [stat("Staging rows (all 7 sources)", f"SELECT sum(row_count) FROM ops.storage_snapshot WHERE layer='staging' AND {LATEST}",
               0, y, 4, color="blue"),
          stat("Mart rows (dims + facts + aggs)", f"SELECT sum(row_count) FROM ops.storage_snapshot WHERE layer='mart' AND {LATEST}",
               4, y, 4, color="green"),
          stat("Postgres database size", "SELECT pg_database_mb FROM ops.storage_footprint ORDER BY snapshot_ts DESC LIMIT 1",
               8, y, 4, unit="decmbytes", decimals=1, color="purple"),
          stat("DuckDB mart file size", "SELECT duckdb_file_mb FROM ops.storage_footprint ORDER BY snapshot_ts DESC LIMIT 1",
               12, y, 4, unit="decmbytes", decimals=1, color="orange", desc="os.path.getsize(sdi_mart.duckdb)"),
          stat("Parquet (partitioned) size", "SELECT parquet_dir_mb FROM ops.storage_footprint ORDER BY snapshot_ts DESC LIMIT 1",
               16, y, 4, unit="decmbytes", decimals=1, color="yellow"),
          stat("Date coverage (sales)", f"""SELECT to_char(min_date,'YYYY-MM') || ' → ' || to_char(max_date,'YYYY-MM') ||
                   ' (' || distinct_months || ' months)' FROM ops.storage_snapshot WHERE table_name='stg_pharmacy_sales_data'
                   AND {LATEST}""", 20, y, 4, color="green",
               desc="MAX(date) - MIN(date) and distinct year-month partitions: validates the 2023-2025 (+2026) span")]
    y += 4
    p += [pie("Storage breakdown: % of staging ROWS by source domain",
              f"SELECT domain, row_count FROM ops.storage_snapshot WHERE layer='staging' AND {LATEST} ORDER BY 2 DESC",
              0, y, 8, 10),
          pie("Storage breakdown: % of staging MB by source domain",
              f"SELECT domain, size_mb FROM ops.storage_snapshot WHERE layer='staging' AND {LATEST} ORDER BY 2 DESC",
              8, y, 8, 10, unit="decmbytes"),
          barchart("Row store vs column store: same data, different footprint (MB)",
                   f"""SELECT 'Sales' AS dataset,
                         max(size_mb) FILTER (WHERE table_name='stg_pharmacy_sales_data') AS "Postgres staging (row store)",
                         max(size_mb) FILTER (WHERE table_name='fact_sales') AS "DuckDB fact (columnar)"
                       FROM ops.storage_snapshot WHERE {LATEST}
                       UNION ALL
                       SELECT 'Hospital demand', max(size_mb) FILTER (WHERE table_name='stg_hospital_demand_data'),
                              max(size_mb) FILTER (WHERE table_name='fact_hospital_demand')
                       FROM ops.storage_snapshot WHERE {LATEST}""", 16, y, 8, 10, unit="decmbytes")]
    y += 10
    p += [barchart("Cumulative staging rows by domain after each refresh (history is appended, never truncated)",
                   """WITH s AS (SELECT DISTINCT ON (refresh_tag, table_name) refresh_tag, domain, row_count
                                 FROM ops.storage_snapshot WHERE layer='staging' AND refresh_tag IS NOT NULL
                                 ORDER BY refresh_tag, table_name, snapshot_ts DESC)
                      SELECT refresh_tag AS refresh,
                        sum(row_count) FILTER (WHERE domain='Pharmacy sales') AS "Pharmacy sales",
                        sum(row_count) FILTER (WHERE domain='IoT telemetry') AS "IoT telemetry",
                        sum(row_count) FILTER (WHERE domain='Warehouse') AS "Warehouse",
                        sum(row_count) FILTER (WHERE domain='Hospital demand') AS "Hospital demand",
                        sum(row_count) FILTER (WHERE domain IN ('Manufacturing','Manufacturer delivery','Medicine master')) AS "Mfg + delivery + master"
                      FROM s GROUP BY 1 ORDER BY 1""", 0, y, 12, 10, stacked=True),
          timeseries("Date coverage: rows per year-month (sales & hospital demand)",
                     """SELECT to_date(year_month || '-01','YYYY-MM-DD')::timestamptz AS time, source_name AS metric,
                               row_count AS value FROM ops.date_coverage
                        WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM ops.date_coverage)
                          AND source_name IN ('05_pharmacy_sales_data','06_hospital_demand_data') ORDER BY 1""",
                     12, y, 12, 10, bars=True,
                     desc="Every month 2023-01 → 2026-02 is present (no holes). The January spikes are the year-start "
                          "bulk postings flagged by the transform layer.")]
    y += 10
    p += [table("Date coverage span per source",
                f"""SELECT table_name, domain, row_count, min_date, max_date, span_days, distinct_months
                    FROM ops.storage_snapshot WHERE layer='staging' AND {LATEST} ORDER BY row_count DESC""", 0, y, 12, 8),
          table("SDI mart tables (DuckDB) — rows & size",
                f"""SELECT table_name, domain AS kind, row_count, size_mb, min_date, max_date, distinct_months
                    FROM ops.storage_snapshot WHERE layer='mart' AND {LATEST} ORDER BY kind DESC, row_count DESC""",
                12, y, 12, 8)]
    for q in p:   # string-valued stat
        if q.get("title") == "Date coverage (sales)":
            q["options"]["reduceOptions"]["fields"] = "/.*/"
    d = dashboard("sdi-storage", "SDI · 3 · Storage & Staging", p,
                  "Table Row Count, Storage Footprint (MB), Date Coverage Span", refresh_var=False, time_from="2023-01-01")
    d["time"] = {"from": "2022-12-01T00:00:00.000Z", "to": "2026-03-31T00:00:00.000Z"}
    return d


# ============================================================================================ 4. QUERY LAYER
def query_dashboard():
    _id[0] = 0
    tier_cols = ("""round(avg(exec_ms) FILTER (WHERE tier='oltp_staging')::numeric,1) AS "Postgres staging (OLTP)",
                    round(avg(exec_ms) FILTER (WHERE tier='star_schema')::numeric,1) AS "Star schema",
                    round(avg(exec_ms) FILTER (WHERE tier='star_optimized')::numeric,1) AS "Optimised star\"""")
    p = [text("### Analytical query layer — 13 business queries on the SDI mart\n"
              "Each query is timed (`time.perf_counter()` around execute + fetch) on three tiers: **Postgres staging** "
              "(what analysts had before the warehouse), the **star schema**, and the **optimised star** (pre-aggregated "
              "summary tables, junk-dimension pre-filters, re-clustered fact). All tiers return identical results "
              "(checked automatically by the benchmark).", 0, 0, 24, 3)]
    y = 3
    p += [stat("Queries executed (selected refreshes)", f"SELECT count(*) FROM ops.query_log WHERE {REFRESH_FILTER}", 0, y, 4),
          stat("Daily active queries (today)", "SELECT count(*) FROM ops.query_log WHERE executed_at >= date_trunc('day', now())",
               4, y, 4, color="green", desc="Counter of every analytical query: benchmark, dashboard refreshes, API, ad-hoc"),
          stat("Query error rate", f"SELECT 100.0*count(*) FILTER (WHERE status='FAILED')/nullif(count(*),0) FROM ops.query_log "
                                  f"WHERE {REFRESH_FILTER}", 8, y, 4, unit="percent", decimals=2,
               thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 5}],
               desc="Failed / total executed x 100 (syntax errors, missing columns/tables, timeouts)"),
          stat("Avg time — Postgres staging", f"SELECT avg(exec_ms) FROM ops.query_log WHERE tier='oltp_staging' AND status='SUCCESS' AND {REFRESH_FILTER}",
               12, y, 4, unit="ms", decimals=1, color="red"),
          stat("Avg time — optimised star", f"SELECT avg(exec_ms) FROM ops.query_log WHERE tier='star_optimized' AND status='SUCCESS' AND {REFRESH_FILTER}",
               16, y, 4, unit="ms", decimals=1, color="green"),
          stat("Speed-up (staging ÷ optimised)", f"""SELECT avg(exec_ms) FILTER (WHERE tier='oltp_staging') /
                   nullif(avg(exec_ms) FILTER (WHERE tier='star_optimized'),0) FROM ops.query_log
                   WHERE status='SUCCESS' AND caller='benchmark' AND {REFRESH_FILTER}""", 20, y, 4, unit="x", decimals=0,
               color="purple")]
    y += 4
    p += [barchart("Average query execution time per dashboard widget (ms, log scale) — by tier",
                   f"""SELECT query_id || ' ' || left(widget_name, 38) AS widget, {tier_cols}
                       FROM ops.query_log WHERE status='SUCCESS' AND caller='benchmark' AND {REFRESH_FILTER}
                       GROUP BY query_id, widget_name ORDER BY query_id""", 0, y, 24, 12, unit="ms", log=True,
                   desc="Recommended chart: identifies unoptimised joins. Tall red bars = row-store scans of 500k+ rows.")]
    y += 12
    p += [barchart("Query count by caller and refresh (daily active queries)",
                   f"""SELECT coalesce(refresh_tag,'none') AS refresh,
                         count(*) FILTER (WHERE caller='benchmark') AS benchmark,
                         count(*) FILTER (WHERE caller='dashboard_refresh') AS "dashboard refresh",
                         count(*) FILTER (WHERE caller='adhoc_analyst') AS "ad-hoc analyst",
                         count(*) FILTER (WHERE caller='api') AS api,
                         count(*) FILTER (WHERE caller='bi_dashboard') AS "BI dashboard (Superset)"
                       FROM ops.query_log WHERE {REFRESH_FILTER} GROUP BY 1 ORDER BY 1""", 0, y, 8, 9, stacked=True),
          timeseries("Query executions over time (per minute, by status)",
                     """SELECT date_trunc('minute', executed_at) AS time, status AS metric, count(*) AS value
                        FROM ops.query_log WHERE $__timeFilter(executed_at) GROUP BY 1,2 ORDER BY 1""",
                     8, y, 8, 9, bars=True),
          barchart("Optimised-tier time per refresh (does the mart stay fast as data grows?)",
                   """SELECT query_id AS query,
                        round(avg(exec_ms) FILTER (WHERE refresh_tag='BASE')::numeric,2) AS "BASE",
                        round(avg(exec_ms) FILTER (WHERE refresh_tag='R1')::numeric,2) AS "R1",
                        round(avg(exec_ms) FILTER (WHERE refresh_tag='R2')::numeric,2) AS "R2"
                      FROM ops.query_log WHERE tier='star_optimized' AND status='SUCCESS' GROUP BY 1 ORDER BY 1""",
                   16, y, 8, 9, unit="ms")]
    y += 9
    p += [table("Query catalog: business question, optimisation and speed-up",
                f"""SELECT query_id, widget_name, business_question,
                           round(avg(exec_ms) FILTER (WHERE tier='oltp_staging')::numeric,1) AS staging_ms,
                           round(avg(exec_ms) FILTER (WHERE tier='star_schema')::numeric,1) AS star_ms,
                           round(avg(exec_ms) FILTER (WHERE tier='star_optimized')::numeric,1) AS optimised_ms,
                           round((avg(exec_ms) FILTER (WHERE tier='oltp_staging') /
                                  nullif(avg(exec_ms) FILTER (WHERE tier='star_optimized'),0))::numeric,1) AS speedup_x
                    FROM ops.query_log WHERE caller='benchmark' AND status='SUCCESS' AND {REFRESH_FILTER}
                    GROUP BY 1,2,3 ORDER BY 1""", 0, y, 16, 10),
          table("Failed queries (error log)",
                f"""SELECT executed_at, caller, query_id, engine, left(error_message, 120) AS error
                    FROM ops.query_log WHERE status='FAILED' AND {REFRESH_FILTER} ORDER BY executed_at DESC""",
                16, y, 8, 10)]
    y += 10
    p += [barchart("BI layer: average execution time per Superset dashboard widget (ms)",
                   f"""SELECT left(widget_name, 55) AS widget, round(avg(exec_ms)::numeric, 1) AS "avg ms",
                              round(max(exec_ms)::numeric, 1) AS "max ms"
                       FROM ops.query_log WHERE caller='bi_dashboard' AND status='SUCCESS' AND {REFRESH_FILTER}
                       GROUP BY widget_name ORDER BY avg(exec_ms) DESC""", 0, y, 16, 14, horizontal=True, unit="ms",
                   desc="Every Superset chart fetch made by DAG sdi_06_bi_refresh (live query through Superset, no cache)."),
          stat("BI chart fetches", f"SELECT count(*) FROM ops.query_log WHERE caller='bi_dashboard' AND {REFRESH_FILTER}",
               16, y, 4, 7, color="green", desc="Counter incremented every time a BI component fetches data"),
          stat("BI avg fetch time", f"SELECT avg(exec_ms) FROM ops.query_log WHERE caller='bi_dashboard' AND status='SUCCESS' "
                                    f"AND {REFRESH_FILTER}", 20, y, 4, 7, unit="ms", decimals=0, color="blue"),
          stat("BI fetch error rate", f"SELECT 100.0*count(*) FILTER (WHERE status='FAILED')/nullif(count(*),0) FROM ops.query_log "
                                      f"WHERE caller='bi_dashboard' AND {REFRESH_FILTER}", 16, y + 7, 8, 7, unit="percent",
               decimals=2, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}])]
    return dashboard("sdi-query-layer", "SDI · 4 · Query Layer & Optimisation", p,
                     "Query Execution Time, Daily Active Query Count, Query Error Rate", time_from="now-24h")


# ============================================================================================ 0. OVERVIEW
def overview_dashboard():
    _id[0] = 0
    p = [text("## SDI Phase 1 — Pipeline overview\n**Problem:** planners cannot see true medicine demand across pharmacy POS, "
              "e-commerce and hospital ERP systems, so hospital forecasts under-shoot consumption and demand is lost "
              "between request and receipt. This pipeline unifies the three demand streams into the **Sales & Demand "
              "Intelligence (SDI) mart**. Dashboards: [Batch](/d/sdi-batch-ingestion) · [Streaming](/d/sdi-stream-ingestion) · "
              "[Storage](/d/sdi-storage) · [Query layer](/d/sdi-query-layer)", 0, 0, 24, 4)]
    y = 4
    p += [table("Refresh cycles at a glance",
                """WITH g AS (SELECT refresh_tag, sum(rows_generated) gen, sum(dirty_rows) dirty FROM ops.generator_log GROUP BY 1),
                        b AS (SELECT refresh_tag, sum(rows_loaded) batch_rows, sum(rows_rejected) batch_rej FROM ops.batch_load_log GROUP BY 1),
                        s AS (SELECT refresh_tag, sum(events_ingested) streamed, sum(events_malformed) stream_bad FROM ops.stream_session GROUP BY 1),
                        t AS (SELECT refresh_tag, max(rows_inserted) FILTER (WHERE target_table='fact_sales') fact_sales_new,
                                     max(rows_inserted) FILTER (WHERE target_table='fact_hospital_demand') fact_hosp_new,
                                     bool_and(status='SUCCESS') FILTER (WHERE step='reconcile') reconciled
                              FROM ops.transform_log GROUP BY 1)
                   SELECT b.refresh_tag AS refresh, g.gen AS rows_generated, g.dirty AS dirty_injected, b.batch_rows,
                          b.batch_rej, s.streamed, s.stream_bad, t.fact_sales_new, t.fact_hosp_new, t.reconciled
                   FROM b LEFT JOIN g USING (refresh_tag) LEFT JOIN s USING (refresh_tag) LEFT JOIN t USING (refresh_tag)
                   ORDER BY 1""", 0, y, 24, 6)]
    y += 6
    p += [barchart("Airflow task duration by DAG (s)",
                   """SELECT dag_id, round(sum(duration_s) FILTER (WHERE refresh_tag IS NULL OR true)::numeric,1) AS "total task seconds",
                             count(*) AS "task runs" FROM ops.pipeline_task_log GROUP BY 1 ORDER BY 1""",
                   0, y, 12, 9, horizontal=True),
          table("Transformation log (dimensions, facts, aggregates, reconciliation)",
                """SELECT logged_at, refresh_tag, step, target_table, rows_in, rows_inserted, rows_updated, duration_s, status,
                          left(details, 80) AS details FROM ops.transform_log ORDER BY transform_id DESC""", 12, y, 12, 9)]
    y += 9
    p += [table("Airflow task outcomes (callbacks)",
                """SELECT logged_at, dag_id, task_id, state, round(duration_s,2) AS duration_s, try_number, run_id
                   FROM ops.pipeline_task_log ORDER BY logged_at DESC""", 0, y, 24, 8)]
    return dashboard("sdi-overview", "SDI · 0 · Pipeline Overview", p, "Phase 1 pipeline overview", refresh_var=False,
                     time_from="now-7d")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for name, d in [("00_overview", overview_dashboard()), ("01_batch_ingestion", batch_dashboard()),
                    ("02_stream_ingestion", stream_dashboard()), ("03_storage", storage_dashboard()),
                    ("04_query_layer", query_dashboard())]:
        (OUT / f"{name}.json").write_text(json.dumps(d, indent=2))
        print("wrote", name, len(d["panels"]), "panels")
