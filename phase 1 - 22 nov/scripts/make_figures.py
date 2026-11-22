"""Builds documentation figures into docs/figures/: architecture, star schema, DAG flow, and preview renders of the
4 Grafana DE dashboards (drawn with matplotlib from the SAME SQL the Grafana panels use, for the written report)."""
import sys
import json
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN

FIG = ROOT / "docs" / "figures"
FIG.mkdir(parents=True, exist_ok=True)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
C = {"src": "#E8EEF7", "ing": "#DCEFE3", "stg": "#FDF0DC", "mart": "#E9E3F5", "q": "#FBE3E3", "ops": "#EEEEEE",
     "edge": "#44546A", "fact": "#2F5597", "dim": "#ffffff", "junk": "#F4E3C3", "role": "#DCEFE3"}


def box(ax, x, y, w, h, text, fc, fs=8, bold=False, ec=None):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08", fc=fc,
                                ec=ec or C["edge"], lw=1))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs, weight="bold" if bold else "normal",
            wrap=True)


def arrow(ax, x1, y1, x2, y2, label=None):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", color=C["edge"], lw=1.2))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.12, label, ha="center", fontsize=6.5, color="#333")


def architecture():
    fig, ax = plt.subplots(figsize=(11, 5.6))
    ax.set_xlim(0, 22); ax.set_ylim(0, 11); ax.axis("off")
    cols = [("1. Oversampling", 0.2, C["src"]), ("2. Ingestion", 4.5, C["ing"]), ("3. Storage", 9.0, C["stg"]),
            ("4. Transform", 13.2, C["mart"]), ("5. Query layer", 17.6, C["q"])]
    for t, x, c in cols:
        ax.text(x + 1.9, 10.5, t, ha="center", fontsize=10, weight="bold")
    box(ax, 0.2, 7.6, 3.8, 2.3, "Oversampling generator\n(DAG sdi_00)\nbootstrap + noise + growth\nlineage-preserving\n~1% dirty rows", C["src"])
    box(ax, 0.2, 4.6, 3.8, 2.3, "7 source files\nBASE workbook (934k rows)\n+ R1 (Jan-26) + R2 (Feb-26)\nExcel increments", C["src"])
    box(ax, 0.2, 1.6, 3.8, 2.3, "stream_queue/\nsales increments\n(~11.3k events / refresh)", C["src"])
    box(ax, 4.5, 7.6, 3.9, 2.3, "File-watcher (watchdog)\non data/landing/\n-> POST /batch/files", C["ing"])
    box(ax, 4.5, 4.6, 3.9, 2.3, "Batch DAG sdi_01\nread Excel (calamine)\nvalidate 9 rule types\nCOPY -> staging", C["ing"])
    box(ax, 4.5, 1.6, 3.9, 2.3, "Ingestion API (FastAPI)\n/stream/{src}/events\nproducer 400 ev/s,\n50-event micro-batches", C["ing"])
    box(ax, 9.0, 5.6, 3.7, 4.3, "PostgreSQL 16\nstaging.* (7 ODS tables,\nappend-only, PK-dedup,\nlineage columns)\n\nrejects.rejected_rows", C["stg"])
    box(ax, 9.0, 1.6, 3.7, 3.4, "ops.* metrics\nbatch_load_log\nstream_* / storage_*\nquery_log, transform_log\npipeline_task_log", C["ops"])
    box(ax, 13.2, 5.6, 3.8, 4.3, "Transform DAG sdi_03\nwatermark delta extract\nSCD1/SCD2 dims, junk dims\nrole-playing date\n2 facts + 5 aggregates\nreconcile -> commit", C["mart"])
    box(ax, 13.2, 1.6, 3.8, 3.4, "DuckDB SDI mart\n(star schema / fact\nconstellation)\n+ partitioned Parquet", C["mart"])
    box(ax, 17.6, 5.6, 4.1, 4.3, "Query DAG sdi_04\n13 business queries\nx 3 tiers (staging /\nstar / optimised)\noptimise: re-cluster,\nART idx, aggregates", C["q"])
    box(ax, 17.6, 1.6, 4.1, 3.4, "Grafana (4 DE dashboards\n+ overview)\nreads ops.* in Postgres", C["q"])
    arrow(ax, 4.0, 8.75, 4.5, 8.75); arrow(ax, 2.1, 7.6, 2.1, 6.9); arrow(ax, 4.0, 5.75, 4.5, 5.75, "")
    arrow(ax, 4.0, 2.75, 4.5, 2.75); arrow(ax, 6.45, 7.6, 6.45, 6.9, "registry")
    arrow(ax, 8.4, 5.75, 9.0, 7.0); arrow(ax, 8.4, 2.75, 9.0, 6.2)
    arrow(ax, 12.7, 7.75, 13.2, 7.75); arrow(ax, 15.1, 5.6, 15.1, 5.0)
    arrow(ax, 17.0, 3.3, 17.6, 7.0)
    ax.plot([10.85, 10.85, 19.65], [1.6, 1.05, 1.05], color=C["edge"], lw=1.2)
    arrow(ax, 19.65, 1.05, 19.65, 1.6)
    ax.text(15.2, 1.15, "ops metrics -> Grafana", ha="center", fontsize=7, color="#333")
    ax.text(11, 0.3, "Orchestration: Apache Airflow 3 (7 DAGs, retries=2, callbacks -> ops.pipeline_task_log); "
                     "sdi_99_refresh_cycle runs a whole refresh end-to-end", ha="center", fontsize=8.5, style="italic")
    fig.savefig(FIG / "architecture.png", dpi=170, bbox_inches="tight"); plt.close(fig)


def star_schema():
    fig, ax = plt.subplots(figsize=(12, 7.4))
    ax.set_xlim(0, 25.2); ax.set_ylim(0, 16.4); ax.axis("off")

    def b(x, y, w, h, t, fc, fs=7.4, color="black", ec=None, lw=1):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.1", fc=fc,
                                    ec=ec or C["edge"], lw=lw, zorder=3))
        ax.text(x + w / 2, y + h / 2, t, ha="center", va="center", fontsize=fs, color=color, zorder=4)

    def link(p, q, col):
        ax.plot([p[0], q[0]], [p[1], q[1]], color=col, lw=1.1, alpha=0.8, zorder=1)

    FS = (5.6, 4.6, 4.8, 6.6)    # fact_sales x, y, w, h
    FH = (15.0, 4.6, 4.8, 6.6)   # fact_hospital_demand
    b(*FS, "FACT_SALES\n(transaction grain)\n~522k rows\n\nqty, gross, discount, net,\ntax, COGS, margin,\nreturned qty\n\ndegenerate: txn / order /\ndelivery / Rx / lot / batch ids",
      C["fact"], fs=7.6, color="white", ec="#1F3864", lw=1.6)
    b(*FH, "FACT_HOSPITAL_DEMAND\n(periodic snapshot:\nhospital x medicine x month)\n~28.7k rows\n\nforecast, requested, approved,\nreceived, consumed, emergency,\nunfulfilled, stockout days,\nforecast error, supply gap",
      C["fact"], fs=7.6, color="white", ec="#1F3864", lw=1.6)
    left = [("dim_pharmacy\nSCD Type 1", 12.6, C["dim"]), ("dim_promotion", 9.4, C["dim"]),
            ("dim_sales_profile  (JUNK)\nsegment, payer, payment,\nRx flag, returns, e-com status,\ndate-quality flag", 6.2, C["junk"]),
            ("dim_warehouse\nconformed for Phase 2/3", 3.0, C["dim"])]
    for t, y, fc in left:
        b(0.3, y, 4.3, 2.4, t, fc)
        link((4.6, y + 1.2), (FS[0], FS[1] + FS[3] / 2), "#2F5597")
    shared = [("dim_date  (role-playing)\nsale · delivery · expiry ·\ndemand-month views", 13.0, C["role"]),
              ("dim_channel\nRetail / Hospital pharmacy /\nE-commerce / Hospital ward", 9.6, C["dim"]),
              ("dim_medicine\nSCD2: price, status, pack\nSCD1: names, class", 6.2, C["dim"]),
              ("dim_geography\ncity · state · census region", 2.8, C["dim"])]
    for t, y, fc in shared:
        b(10.75, y, 3.9, 2.4, t, fc, fs=7.0)
        link((10.75, y + 1.2), (FS[0] + FS[2], FS[1] + FS[3] / 2), "#2F5597")
        link((14.65, y + 1.2), (FH[0], FH[1] + FH[3] / 2), "#C55A11")
    right = [("dim_hospital\nSCD2: beds, ICU, occupancy,\ntrauma level, catchment", 10.8, C["dim"]),
             ("dim_demand_profile  (JUNK)\nrequest status, formulary,\nemergency / stockout /\nunfulfilled flags", 4.4, C["junk"])]
    for t, y, fc in right:
        b(20.6, y, 4.3, 2.6, t, fc)
        link((20.6, y + 1.3), (FH[0] + FH[2], FH[1] + FH[3] / 2), "#C55A11")
    ax.text(2.45, 15.4, "sales-only dimensions", ha="center", fontsize=8, style="italic")
    ax.text(12.7, 15.9, "conformed dimensions (shared)", ha="center", fontsize=8, style="italic")
    ax.text(22.75, 15.4, "hospital-only dimensions", ha="center", fontsize=8, style="italic")
    ax.text(12.6, 17.0, "SDI data mart: fact constellation (two facts sharing conformed dimensions)", ha="center",
            fontsize=11.5, weight="bold")
    ax.text(12.6, 16.45, "blue = fact_sales foreign keys  ·  orange = fact_hospital_demand foreign keys  ·  "
                         "beige = junk dimension  ·  green = role-playing", ha="center", fontsize=8)
    ax.text(12.6, 1.6, "Aggregates refreshed by Airflow after every load (materialised-view equivalents): agg_sales_monthly · "
                       "agg_sales_daily_channel · agg_hospital_monthly · agg_unified_demand_monthly (drill-across) · "
                       "agg_medicine_velocity", ha="center", fontsize=7.6, style="italic")
    fig.savefig(FIG / "star_schema.png", dpi=170, bbox_inches="tight"); plt.close(fig)


def dag_flow():
    fig, ax = plt.subplots(figsize=(11, 3.4))
    ax.set_xlim(0, 24); ax.set_ylim(0, 6.5); ax.axis("off")
    steps = [("sdi_00\ndata_generator", "generate -> validate\n-> log stats -> publish"),
             ("sdi_01\nbatch_ingestion", "discover -> master first\n-> load_file.expand()\n-> summarize"),
             ("sdi_02\nstream_ingestion", "queue check -> API sensor\n-> stream events\n-> validate session"),
             ("sdi_03\ntransform", "init -> delta -> dims\n-> facts -> aggregates\n-> reconcile+commit"),
             ("sdi_04\nquery_layer", "optimise -> benchmark\n-> dashboard refresh\n-> ad-hoc sim"),
             ("sdi_05\nstorage_metrics", "row counts, MB,\ndate coverage\n-> coverage check")]
    for i, (t, d) in enumerate(steps):
        x = 0.3 + i * 4.0
        box(ax, x, 3.3, 3.3, 1.7, t, C["mart"], fs=8.5, bold=True)
        ax.text(x + 1.65, 2.3, d, ha="center", va="center", fontsize=7)
        if i < len(steps) - 1:
            arrow(ax, x + 3.3, 4.15, x + 4.0, 4.15)
    ax.text(12, 6.0, "sdi_99_refresh_cycle (TriggerDagRunOperator, wait_for_completion) chains the 6 DAGs; each DAG also "
                     "triggers the next and has its own schedule", ha="center", fontsize=8.3, weight="bold")
    ax.text(12, 0.6, "Schedules: batch */2 min (file-registry poll, plus immediate watcher trigger) · stream */5 min · "
                     "transform @hourly · query daily 06:00 · storage @hourly · retries=2, retry_delay=1 min, timeout 30 min",
            ha="center", fontsize=7.6, style="italic")
    fig.savefig(FIG / "dag_flow.png", dpi=170, bbox_inches="tight"); plt.close(fig)


# --------------------------------------------------------------------------------- dashboard preview renders
def q(sql):
    sql = sql.replace("${refresh:sqlstring}", "'BASE','R1','R2'")
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 > now() - interval '3650 days'", sql)
    with psycopg2.connect(PG_DSN) as c:
        return pd.read_sql(sql, c)


def panels_of(name):
    d = json.loads((ROOT / "grafana/dashboards" / f"{name}.json").read_text())
    return {p["title"]: p for p in d["panels"] if p.get("targets")}, d["title"]


def stat_row(ax_list, pairs):
    for ax, (label, val) in zip(ax_list, pairs):
        ax.axis("off")
        ax.add_patch(FancyBboxPatch((0.02, 0.05), 0.96, 0.9, boxstyle="round,pad=0.01", fc="#2F5597", ec="none",
                                    transform=ax.transAxes))
        ax.text(0.5, 0.62, val, ha="center", va="center", color="white", fontsize=13, weight="bold", transform=ax.transAxes)
        ax.text(0.5, 0.22, label, ha="center", va="center", color="white", fontsize=6.2, transform=ax.transAxes)


def preview_batch():
    P, title = panels_of("01_batch_ingestion")
    s = lambda t: q(P[t]["targets"][0]["rawSql"]).iloc[0, 0]
    fig = plt.figure(figsize=(11, 6.6))
    top = fig.add_gridspec(1, 6, top=0.93, bottom=0.85, wspace=0.12)
    gs = fig.add_gridspec(2, 2, top=0.78, bottom=0.1, hspace=0.55, wspace=0.62)
    stat_row([fig.add_subplot(top[0, i]) for i in range(6)],
             [("Files/sheets loaded", f"{int(s('Files / sheets loaded')):,}"), ("Rows loaded", f"{int(s('Rows loaded')):,}"),
              ("Batch success rate", f"{s('Batch success rate'):.1f}%"), ("Rows rejected", f"{int(s('Rows rejected by validation')):,}"),
              ("Duplicates skipped", f"{int(s('Duplicates skipped (idempotent)')):,}"), ("Total load time", f"{float(s('Total load time')):.0f}s")])
    ax = fig.add_subplot(gs[0, 0])
    d = q(P["Batch load duration per Excel source (seconds)"]["targets"][0]["rawSql"]).head(12).iloc[::-1]
    ax.barh(d.source, d["duration (s)"], color="#2F5597"); ax.set_title("Load duration per Excel source (s)", fontsize=9)
    ax.tick_params(labelsize=6.5)
    ax = fig.add_subplot(gs[0, 1])
    d = q(P["Time breakdown per source (BASE load): read vs validate vs insert"]["targets"][0]["rawSql"]).iloc[::-1]
    left = 0
    for col, c in zip(["read Excel", "validate", "insert (COPY)"], ["#2F5597", "#C55A11", "#548235"]):
        ax.barh(d.source, d[col].astype(float), left=left, label=col, color=c); left = left + d[col].astype(float)
    ax.legend(fontsize=6.5); ax.set_title("BASE load: read vs validate vs insert (s)", fontsize=9); ax.tick_params(labelsize=6.5)
    ax = fig.add_subplot(gs[1, 0])
    d = q(P["Rows processed per batch (log scale) by refresh"]["targets"][0]["rawSql"]).set_index("source")
    d.plot(kind="bar", ax=ax, logy=True, rot=25, color=["#2F5597", "#C55A11", "#548235"], width=0.8)
    ax.set_title("Rows processed per batch (log)", fontsize=9); ax.tick_params(labelsize=6); ax.set_xlabel("")
    ax.legend(fontsize=6.5)
    ax = fig.add_subplot(gs[1, 1])
    d = q(P["Rejected rows by validation rule"]["targets"][0]["rawSql"]).set_index("rule")
    d.plot(kind="barh", ax=ax, color=["#C55A11", "#7030A0"]); ax.set_title("Rejected rows by validation rule", fontsize=9)
    ax.tick_params(labelsize=6.5); ax.legend(fontsize=6.5); ax.set_ylabel("")
    fig.suptitle(title + "  (preview render of the Grafana panels' SQL)", fontsize=10, weight="bold")
    fig.savefig(FIG / "dash_batch.png", dpi=160, bbox_inches="tight"); plt.close(fig)


def preview_stream():
    P, title = panels_of("02_stream_ingestion")
    s = lambda t: q(P[t]["targets"][0]["rawSql"]).iloc[0, 0]
    fig = plt.figure(figsize=(11, 5.8))
    gs = fig.add_gridspec(3, 6, height_ratios=[0.5, 2, 1.8], hspace=0.6, wspace=0.6)
    stat_row([fig.add_subplot(gs[0, i]) for i in range(6)],
             [("Events ingested", f"{s('Events ingested'):,.0f}"), ("Throughput", f"{s('Throughput (events/sec)'):.0f} ev/s"),
              ("Avg latency", f"{s('Avg end-to-end latency'):.0f} ms"), ("p95 latency", f"{s('p95 latency'):.0f} ms"),
              ("Malformed", f"{s('Malformed / dropped events'):,.0f}"), ("Duplicates", f"{s('Duplicate events rejected'):,.0f}")])
    d = q(P["Stream throughput (events/sec) during the simulation"]["targets"][0]["rawSql"])
    for i, (tag, g) in enumerate(d.groupby("metric")):
        ax = fig.add_subplot(gs[1, i * 3:(i + 1) * 3])
        t = (pd.to_datetime(g.time) - pd.to_datetime(g.time).min()).dt.total_seconds()
        ax.plot(t, g.value, marker="o", ms=2.5, color="#2F5597"); ax.axhline(400, ls="--", color="#C55A11", lw=0.8)
        ax.set_ylim(0, 520); ax.set_title(f"Throughput events/s — session {tag} (target 400)", fontsize=9)
        ax.set_xlabel("seconds into stream", fontsize=7)
    ax = fig.add_subplot(gs[2, :3])
    d = q(P["Latency distribution (all events)"]["targets"][0]["rawSql"])
    ax.hist(d.latency_ms.clip(upper=400), bins=40, color="#548235"); ax.set_title("End-to-end latency distribution (ms)", fontsize=9)
    ax = fig.add_subplot(gs[2, 3:])
    d = q(P["Per-session summary"]["targets"][0]["rawSql"]).set_index("session")
    d.plot(kind="bar", ax=ax, logy=True, rot=0, color=["#548235", "#C55A11", "#7030A0"]); ax.legend(fontsize=6.5)
    ax.set_title("Per session: ingested vs malformed vs duplicate (log)", fontsize=9); ax.set_xlabel("")
    fig.suptitle(title + "  (preview render of the Grafana panels' SQL)", fontsize=10, weight="bold")
    fig.savefig(FIG / "dash_stream.png", dpi=160, bbox_inches="tight"); plt.close(fig)


def preview_storage():
    P, title = panels_of("03_storage")
    s = lambda t: q(P[t]["targets"][0]["rawSql"]).iloc[0, 0]
    fig = plt.figure(figsize=(11, 6.0))
    gs = fig.add_gridspec(3, 6, height_ratios=[0.5, 2.1, 1.9], hspace=0.6, wspace=0.7)
    stat_row([fig.add_subplot(gs[0, i]) for i in range(6)],
             [("Staging rows", f"{s('Staging rows (all 7 sources)'):,.0f}"), ("Mart rows", f"{s('Mart rows (dims + facts + aggs)'):,.0f}"),
              ("Postgres DB", f"{s('Postgres database size'):.0f} MB"), ("DuckDB mart", f"{s('DuckDB mart file size'):.0f} MB"),
              ("Parquet", f"{s('Parquet (partitioned) size'):.0f} MB"), ("Sales coverage 2023-01 →", s("Date coverage (sales)").split(" (")[1].rstrip(")"))])
    for i, (t, col) in enumerate([("Storage breakdown: % of staging ROWS by source domain", "row_count"),
                                  ("Storage breakdown: % of staging MB by source domain", "size_mb")]):
        ax = fig.add_subplot(gs[1, i * 2:(i + 1) * 2])
        d = q(P[t]["targets"][0]["rawSql"])
        ax.pie(d[col].astype(float), labels=None, autopct=lambda v: f"{v:.0f}%" if v > 4 else "", pctdistance=0.78,
               wedgeprops=dict(width=0.42), textprops={"fontsize": 6.5})
        ax.set_title(("Rows" if i == 0 else "MB") + " by source domain", fontsize=9)
        if i == 1:
            ax.legend(d.domain, fontsize=6, loc="center left", bbox_to_anchor=(1, 0.5))
    ax = fig.add_subplot(gs[1, 4:])
    d = q(P["Row store vs column store: same data, different footprint (MB)"]["targets"][0]["rawSql"]).set_index("dataset")
    d.astype(float).plot(kind="bar", ax=ax, rot=0, color=["#C55A11", "#2F5597"]); ax.legend(fontsize=6)
    ax.set_title("Row store vs column store (MB)", fontsize=9); ax.set_xlabel("")
    ax = fig.add_subplot(gs[2, :3])
    d = q(P["Cumulative staging rows by domain after each refresh (history is appended, never truncated)"]["targets"][0]["rawSql"]).set_index("refresh")
    d.astype(float).plot(kind="bar", stacked=True, ax=ax, rot=0); ax.legend(fontsize=5.5, ncol=2)
    ax.set_title("Cumulative staging rows after each refresh", fontsize=9); ax.set_xlabel("")
    ax = fig.add_subplot(gs[2, 3:])
    d = q(P["Date coverage: rows per year-month (sales & hospital demand)"]["targets"][0]["rawSql"])
    for m, g in d.groupby("metric"):
        ax.bar(pd.to_datetime(g.time), g.value, width=20, label=m, alpha=0.75)
    ax.legend(fontsize=6); ax.set_title("Date coverage: rows per month 2023-01 → 2026-02", fontsize=9); ax.tick_params(labelsize=6.5)
    fig.suptitle(title + "  (preview render of the Grafana panels' SQL)", fontsize=10, weight="bold")
    fig.savefig(FIG / "dash_storage.png", dpi=160, bbox_inches="tight"); plt.close(fig)


def preview_query():
    P, title = panels_of("04_query_layer")
    s = lambda t: q(P[t]["targets"][0]["rawSql"]).iloc[0, 0]
    fig = plt.figure(figsize=(11, 5.8))
    gs = fig.add_gridspec(3, 6, height_ratios=[0.5, 2.4, 1.6], hspace=0.75, wspace=0.6)
    stat_row([fig.add_subplot(gs[0, i]) for i in range(6)],
             [("Queries executed", f"{s('Queries executed (selected refreshes)'):,}"),
              ("Daily active queries", f"{s('Daily active queries (today)'):,}"),
              ("Query error rate", f"{s('Query error rate'):.2f}%"),
              ("Avg staging (OLTP)", f"{s('Avg time — Postgres staging'):.0f} ms"),
              ("Avg optimised star", f"{s('Avg time — optimised star'):.1f} ms"),
              ("Speed-up", f"{s('Speed-up (staging ÷ optimised)'):.0f}x")])
    ax = fig.add_subplot(gs[1, :])
    d = q(P["Average query execution time per dashboard widget (ms, log scale) — by tier"]["targets"][0]["rawSql"]).set_index("widget")
    d.index = [i[:3] for i in d.index]
    d.astype(float).plot(kind="bar", ax=ax, logy=True, rot=0, color=["#C00000", "#2F5597", "#548235"], width=0.8)
    ax.set_title("Average execution time per dashboard widget by tier (ms, log scale)", fontsize=9); ax.legend(fontsize=7)
    ax = fig.add_subplot(gs[2, :3])
    d = q(P["Query count by caller and refresh (daily active queries)"]["targets"][0]["rawSql"]).set_index("refresh")
    d.astype(float).plot(kind="bar", stacked=True, ax=ax, rot=0); ax.legend(fontsize=6); ax.set_xlabel("")
    ax.set_title("Query count by caller and refresh", fontsize=9)
    ax = fig.add_subplot(gs[2, 3:])
    d = q(P["Optimised-tier time per refresh (does the mart stay fast as data grows?)"]["targets"][0]["rawSql"]).set_index("query")
    d.astype(float).plot(kind="bar", ax=ax, rot=0, width=0.8); ax.legend(fontsize=6); ax.set_xlabel("")
    ax.set_title("Optimised tier per refresh (ms)", fontsize=9); ax.tick_params(labelsize=6.5)
    fig.suptitle(title + "  (preview render of the Grafana panels' SQL)", fontsize=10, weight="bold")
    fig.savefig(FIG / "dash_query.png", dpi=160, bbox_inches="tight"); plt.close(fig)


if __name__ == "__main__":
    architecture(); star_schema(); dag_flow()
    preview_batch(); preview_stream(); preview_storage(); preview_query()
    print("figures:", sorted(p.name for p in FIG.glob("*.png")))
