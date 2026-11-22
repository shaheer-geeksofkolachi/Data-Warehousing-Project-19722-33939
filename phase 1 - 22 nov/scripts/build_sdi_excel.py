"""Builds docs/SDI.Excel.xlsx - the complete Phase-1 SDI design & evidence workbook - from the LIVE pipeline state
(Postgres staging + ops metrics, DuckDB mart, source files, and the code's own registries).
Run after a full BASE + R1 + R2 cycle:   python scripts/build_sdi_excel.py"""
import sys
import json
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN, DUCKDB_PATH, SOURCE_DIR, REFRESHES
from sdi.sources import SOURCES, snake
from sdi.query.catalog import QUERIES, ADHOC
from sdi.warehouse.transform import MEDICINE_T1, MEDICINE_T2, HOSPITAL_T2, AGGREGATES_SQL
from sdi.generator.oversample import SCENARIOS, DIRTY_RATE, YOY_GROWTH

OUT = ROOT / "docs" / "SDI.Excel.xlsx"
FIG = ROOT / "docs" / "figures"
FONT = "Arial"
NAVY, BLUE, LIGHT, GREY, YELLOW = "1F3864", "2F5597", "DDEBF7", "F2F2F2", "FFF2CC"
thin = Side(style="thin", color="BFBFBF")
BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)

pg = psycopg2.connect(PG_DSN)
duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)


def pgdf(sql):
    return pd.read_sql(sql, pg)


def dkdf(sql):
    return duck.execute(sql).df()


wb = Workbook()
wb.remove(wb.active)
TOC = []
_tables = [0]


def sheet(name, title, purpose):
    ws = wb.create_sheet(name)
    ws.sheet_view.showGridLines = False
    ws["A1"] = title
    ws["A1"].font = Font(name=FONT, size=14, bold=True, color=NAVY)
    ws["A2"] = purpose
    ws["A2"].font = Font(name=FONT, size=9, italic=True, color="595959")
    ws["A2"].alignment = Alignment(wrap_text=False)
    TOC.append((name, title, purpose))
    return ws


def section(ws, row, text, col=1):
    c = ws.cell(row=row, column=col, value=text)
    c.font = Font(name=FONT, size=11, bold=True, color=BLUE)
    return row + 1


def write_df(ws, df, row, col=1, name=None, widths=None, wrap_cols=(), number_formats=None, max_width=60):
    """Writes a DataFrame as a formatted Excel table. Returns the next free row."""
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.tz_localize(None) if getattr(df[c].dt, "tz", None) is not None else df[c]
    hdr_fill = PatternFill("solid", fgColor=BLUE)
    for j, h in enumerate(df.columns):
        c = ws.cell(row=row, column=col + j, value=str(h))
        c.font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
        c.fill = hdr_fill
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = BORDER
    for i, r in enumerate(df.itertuples(index=False), start=1):
        for j, v in enumerate(r):
            if v is not None and not isinstance(v, str):
                try:
                    if pd.isna(v):
                        v = None
                except (TypeError, ValueError):
                    pass
            if hasattr(v, "item") and not isinstance(v, str):
                v = v.item()
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime()
            c = ws.cell(row=row + i, column=col + j, value=v)
            c.font = Font(name=FONT, size=9)
            c.border = BORDER
            c.alignment = Alignment(vertical="top", wrap_text=df.columns[j] in wrap_cols)
            if isinstance(v, float):
                c.number_format = "#,##0.00"
            elif isinstance(v, int) and not isinstance(v, bool):
                c.number_format = "#,##0"
            elif isinstance(v, dt.datetime):
                c.number_format = "yyyy-mm-dd hh:mm:ss"
            elif isinstance(v, dt.date):
                c.number_format = "yyyy-mm-dd"
            if number_formats and df.columns[j] in number_formats:
                c.number_format = number_formats[df.columns[j]]
    if len(df) and name:
        _tables[0] += 1
        ref = f"{get_column_letter(col)}{row}:{get_column_letter(col + len(df.columns) - 1)}{row + len(df)}"
        t = Table(displayName=f"t{_tables[0]}_{name}"[:250], ref=ref)
        t.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        ws.add_table(t)
    for j, h in enumerate(df.columns):
        letter = get_column_letter(col + j)
        if widths and h in widths:
            w = widths[h]
        else:
            sample = [len(str(h))] + [len(str(x)) for x in df[h].head(200).tolist()]
            w = min(max(sample) + 2, max_width)
        ws.column_dimensions[letter].width = max(ws.column_dimensions[letter].width or 0, w, 8)
    return row + len(df) + 2


def note(ws, row, text, col=1, width_cols=10, italic=True, size=9, bold=False, color="404040"):
    c = ws.cell(row=row, column=col, value=text)
    c.font = Font(name=FONT, size=size, italic=italic, bold=bold, color=color)
    c.alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + width_cols - 1)
    ws.row_dimensions[row].height = max(15, 13 * (1 + len(text) // (width_cols * 14)))
    return row + 1


def kv(ws, row, pairs, col=1, key_w=34, val_w=70):
    for k, v in pairs:
        a = ws.cell(row=row, column=col, value=k)
        a.font = Font(name=FONT, size=9, bold=True)
        a.fill = PatternFill("solid", fgColor=LIGHT)
        a.border = BORDER
        a.alignment = Alignment(vertical="top", wrap_text=True)
        b = ws.cell(row=row, column=col + 1, value=v)
        b.font = Font(name=FONT, size=9)
        b.border = BORDER
        b.alignment = Alignment(vertical="top", wrap_text=True)
        row += 1
    ws.column_dimensions[get_column_letter(col)].width = max(ws.column_dimensions[get_column_letter(col)].width or 0, key_w)
    ws.column_dimensions[get_column_letter(col + 1)].width = max(ws.column_dimensions[get_column_letter(col + 1)].width or 0, val_w)
    return row + 1


def formula_cell(ws, ref, formula, fmt="#,##0", bold=False):
    ws[ref] = formula
    ws[ref].font = Font(name=FONT, size=9, bold=bold)
    ws[ref].number_format = fmt
    ws[ref].border = BORDER


def label(ws, ref, text, bold=True, fill=LIGHT):
    ws[ref] = text
    ws[ref].font = Font(name=FONT, size=9, bold=bold)
    ws[ref].fill = PatternFill("solid", fgColor=fill)
    ws[ref].border = BORDER
    ws[ref].alignment = Alignment(wrap_text=True, vertical="top")


def image(ws, path, anchor, width=None):
    if Path(path).exists():
        img = XLImage(str(path))
        if width:
            ratio = width / img.width
            img.width, img.height = width, img.height * ratio
        ws.add_image(img, anchor)


# ============================================================================== data pulls
batch = pgdf("SELECT * FROM ops.batch_load_log ORDER BY load_id")
stream_s = pgdf("SELECT * FROM ops.stream_session ORDER BY start_ts")
stream_m = pgdf("SELECT session_id, bucket_ts, events_ingested, events_malformed, events_duplicate, "
                "round((latency_sum_ms/nullif(events_ingested,0))::numeric,2) AS avg_latency_ms, "
                "round(latency_max_ms::numeric,2) AS max_latency_ms FROM ops.stream_metrics ORDER BY bucket_ts")
gen = pgdf("SELECT refresh_tag, source_name, target_month, rows_generated, dirty_rows, generated_at "
           "FROM ops.generator_log ORDER BY refresh_tag, source_name")
storage = pgdf("SELECT * FROM ops.storage_snapshot ORDER BY snapshot_id")
footprint = pgdf("SELECT * FROM ops.storage_footprint ORDER BY snapshot_ts")
coverage = pgdf("SELECT * FROM ops.date_coverage WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM ops.date_coverage)")
tlog = pgdf("SELECT * FROM ops.transform_log ORDER BY transform_id")
qlog = pgdf("SELECT * FROM ops.query_log ORDER BY query_log_id")
rejects = pgdf("SELECT source_name, ingest_mode, reason_code, coalesce(r.refresh_tag, b.refresh_tag, s.refresh_tag) AS refresh_tag, "
               "count(*) AS rows FROM rejects.rejected_rows x "
               "LEFT JOIN (SELECT DISTINCT batch_id, refresh_tag FROM ops.batch_load_log) b USING (batch_id) "
               "LEFT JOIN (SELECT session_id AS batch_id, refresh_tag FROM ops.stream_session) s USING (batch_id) "
               "LEFT JOIN (SELECT NULL::text AS batch_id, NULL::text AS refresh_tag) r USING (batch_id) "
               "GROUP BY 1,2,3,4 ORDER BY 4,1,3")
reject_samples = pgdf("SELECT source_name, ingest_mode, reason_code, reason_detail, left(record::text, 220) AS record_excerpt "
                      "FROM (SELECT *, row_number() OVER (PARTITION BY source_name, reason_code ORDER BY reject_id) rn "
                      "FROM rejects.rejected_rows) x WHERE rn <= 2 ORDER BY 1,3")
tasklog = pgdf("SELECT * FROM ops.pipeline_task_log ORDER BY logged_at")
registry = pgdf("SELECT * FROM ops.file_registry ORDER BY file_id")

# ============================================================================== 00 Cover
ws = sheet("00_Cover", "SDI.Excel — Sales & Demand Intelligence (SDI) Data Mart · Phase 1",
           "Data Analytics & Warehousing (Fall 2026) · Pharmaceutical Cold Supply Chain · automated DWH + BI pipeline on Apache Airflow")
r = 4
r = kv(ws, r, [
    ("Project", "An Automated DWH + BI Pipeline based on Data Engineering — Phase 1: Sales & Demand Intelligence (SDI) Data Mart"),
    ("Scope of this milestone", "Implemented up to the Query layer: oversampling -> batch + streaming ingestion (API + file-watcher) -> "
                                "Postgres staging (ODS) -> DuckDB star schema (fact constellation) -> optimised analytical query layer, "
                                "all orchestrated by Apache Airflow 3 and observed through 4 Grafana DE dashboards."),
    ("Business problem", "Planners cannot see true medicine demand: pharmacy POS, e-commerce and hospital ERP data arrive separately at "
                         "different grains, so hospital forecasts under-shoot consumption, demand is lost between request and receipt, "
                         "and production is planned on incomplete signals. (Sheet 01_Problem_KPIs)"),
    ("Workbook generated", dt.datetime.now().strftime("%Y-%m-%d %H:%M") + " from the live pipeline state (scripts/build_sdi_excel.py)"),
    ("Pipeline runs captured", "BASE (provided 2023-2025 history) + R1 (Jan-2026) + R2 (Feb-2026) - every number in this workbook "
                               "comes from those three real runs"),
])
_gcsv = ROOT / "docs" / "group_members.csv"
_g = pd.read_csv(_gcsv, dtype=str).fillna("") if _gcsv.exists() else pd.DataFrame(columns=["name"])
_g = _g[_g.get("name", pd.Series(dtype=str)).str.strip() != ""] if len(_g) else _g
r = section(ws, r, "Group members" if len(_g) else "Group members (fill in)")
_n = len(_g) or 4
_col = lambda c: list(_g[c]) + [""] * (_n - len(_g)) if c in _g else [""] * _n
gm = pd.DataFrame({"#": list(range(1, _n + 1)), "Name": _col("name"), "ERP / Student ID": _col("student_id")})
start = r
r = write_df(ws, gm, r, widths={"Name": 28, "ERP / Student ID": 18})
if len(_g) == 0:
    for rr in range(start + 1, start + 5):
        for cc in range(2, 4):
            ws.cell(row=rr, column=cc).fill = PatternFill("solid", fgColor=YELLOW)
    note(ws, r - 1, "Yellow cells = to be filled in by the group (or fill docs/group_members.csv and rebuild).", width_cols=3)
r += 1
r = section(ws, r, "Headline numbers (from the live pipeline)")
fs_rows = duck.execute("SELECT count(*), round(sum(net_sales_usd),2) FROM fact_sales").fetchone()
fh_rows = duck.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]
qb = qlog[(qlog.caller == "benchmark") & (qlog.status == "SUCCESS")]
r = kv(ws, r, [
    ("Staging rows loaded (7 sources, all refreshes)", f"{int(storage[storage.layer=='staging'].groupby('table_name').row_count.last().sum()):,}"),
    ("fact_sales rows / net sales", f"{fs_rows[0]:,} rows · ${fs_rows[1]:,.2f}"),
    ("fact_hospital_demand rows", f"{fh_rows:,}"),
    ("Batch sheets loaded / success rate", f"{len(batch)} / {100*(batch.status=='SUCCESS').mean():.1f}%"),
    ("Streaming", f"{int(stream_s.events_ingested.sum()):,} events ingested at ~{stream_s.throughput_eps.mean():.0f} events/s, "
                  f"avg latency {stream_s.avg_latency_ms.mean():.0f} ms, {int(stream_s.events_malformed.sum())} malformed caught"),
    ("Query layer", f"{len(qlog):,} timed executions · Postgres staging avg {qb[qb.tier=='oltp_staging'].exec_ms.mean():.0f} ms vs "
                    f"optimised star avg {qb[qb.tier=='star_optimized'].exec_ms.mean():.1f} ms"),
    ("Reconciliation", "staging = mart after every refresh (row counts and net-sales $ match exactly; 0 orphan keys)"),
])
r = section(ws, r, "Contents")
toc_row = r
ws.column_dimensions["A"].width = 34

# ============================================================================== 01 Problem & KPIs
ws = sheet("01_Problem_KPIs", "Problem statement, evidence and KPIs",
           "Why the SDI mart exists. Evidence numbers are computed by formulas from the raw totals (BASE history 2023-2025).")
h = pgdf("""SELECT sum(requested_quantity_packs) req, sum(approved_quantity_packs) appr, sum(received_quantity_packs) recv,
                   sum(consumed_quantity_packs) cons, sum(forecast_demand_packs) fc, sum(emergency_request_quantity_packs) emerg,
                   count(*) FILTER (WHERE emergency_request_quantity_packs>0) emerg_rows, count(*) rows_,
                   sum(unfulfilled_demand_packs) unful, sum(stockout_days) so_days
            FROM staging.stg_hospital_demand_data WHERE _refresh_tag='BASE'""").iloc[0]
so_pain = pgdf("""SELECT sum(d.stockout_days) FROM staging.stg_hospital_demand_data d WHERE _refresh_tag='BASE'
                  AND disease_category='Pain and Inflammatory Conditions'""").iloc[0, 0]
unf_pac = pgdf("""SELECT sum(unfulfilled_demand_packs) FROM staging.stg_hospital_demand_data WHERE _refresh_tag='BASE'
                  AND hospital_name='Pacific University Hospital'""").iloc[0, 0]
r = 4
r = note(ws, r, "PROBLEM: Clarity Pharma's planners cannot see true medicine demand, so hospitals are under-supplied and "
                "production is planned on incomplete signals. Demand arrives through three disconnected systems at different "
                "grains: pharmacy POS (transactions), the e-commerce platform (orders) and hospital ERP exports (hospital x medicine "
                "x month). Nobody can answer: 'how much of medicine X is region Y consuming across ALL channels, and is our "
                "forecast keeping up?'", width_cols=8, italic=False, size=10, bold=True, color=NAVY)
r += 1
r = section(ws, r, "Evidence from the data (raw inputs in blue, results by formula)")
label(ws, f"A{r}", "Measure"); label(ws, f"B{r}", "Value"); label(ws, f"C{r}", "How computed / meaning")
ev = [("Hospital packs requested", int(h.req), "input"), ("Hospital packs approved", int(h.appr), "input"),
      ("Hospital packs received", int(h.recv), "input"), ("Hospital packs consumed", int(h.cons), "input"),
      ("Hospital packs forecast", int(h.fc), "input"), ("Emergency request packs", int(h.emerg), "input"),
      ("Hospital-months with emergency requests", int(h.emerg_rows), "input"), ("Hospital-month records", int(h.rows_), "input"),
      ("Unfulfilled demand packs", int(h.unful), "input"), ("Stock-out days (all)", int(h.so_days), "input"),
      ("Stock-out days — Pain & Inflammatory", int(so_pain), "input"),
      ("Unfulfilled packs — Pacific University Hospital", int(unf_pac), "input")]
base_r = r + 1
for i, (k, v, _) in enumerate(ev):
    ws.cell(row=base_r + i, column=1, value=k).font = Font(name=FONT, size=9)
    c = ws.cell(row=base_r + i, column=2, value=v)
    c.font = Font(name=FONT, size=9, color="0000FF"); c.number_format = "#,##0"; c.border = BORDER
    ws.cell(row=base_r + i, column=3, value="Source: staging.stg_hospital_demand_data, BASE load (2023-2025)").font = Font(name=FONT, size=8, italic=True)
R = {k: base_r + i for i, (k, _, _) in enumerate(ev)}
fr = base_r + len(ev) + 1
derived = [
    ("Supply gap: requested - received (packs)", f"=B{R['Hospital packs requested']}-B{R['Hospital packs received']}", "#,##0",
     "Demand lost between request and receipt"),
    ("Supply gap as % of requested", f"=(B{R['Hospital packs requested']}-B{R['Hospital packs received']})/B{R['Hospital packs requested']}", "0.0%", ""),
    ("Lost at approval (% of requested)", f"=(B{R['Hospital packs requested']}-B{R['Hospital packs approved']})/B{R['Hospital packs requested']}", "0.0%", ""),
    ("Lost in delivery (% of requested)", f"=(B{R['Hospital packs approved']}-B{R['Hospital packs received']})/B{R['Hospital packs requested']}", "0.0%", ""),
    ("Forecast bias (forecast - consumed) / consumed", f"=(B{R['Hospital packs forecast']}-B{R['Hospital packs consumed']})/B{R['Hospital packs consumed']}", "0.00%",
     "Negative = systematic under-forecast (true in all 7 disease categories)"),
    ("Emergency packs as % of consumption", f"=B{R['Emergency request packs']}/B{R['Hospital packs consumed']}", "0.0%", ""),
    ("Share of hospital-months with emergencies", f"=B{R['Hospital-months with emergency requests']}/B{R['Hospital-month records']}", "0.0%", ""),
    ("Pain & Inflammatory share of stock-out days", f"=B{R['Stock-out days — Pain & Inflammatory']}/B{R['Stock-out days (all)']}", "0.0%", ""),
    ("Pacific University Hospital share of unfulfilled", f"=B{R['Unfulfilled packs — Pacific University Hospital']}/B{R['Unfulfilled demand packs']}", "0.0%", ""),
]
label(ws, f"A{fr}", "Derived evidence"); label(ws, f"B{fr}", "Result"); label(ws, f"C{fr}", "Interpretation")
for i, (k, f, fmt, m) in enumerate(derived, start=1):
    ws.cell(row=fr + i, column=1, value=k).font = Font(name=FONT, size=9, bold=True)
    formula_cell(ws, f"B{fr + i}", f, fmt, bold=True)
    ws.cell(row=fr + i, column=3, value=m).font = Font(name=FONT, size=9)
ws.column_dimensions["A"].width = 48; ws.column_dimensions["B"].width = 16; ws.column_dimensions["C"].width = 70
r = fr + len(derived) + 2
r = section(ws, r, "Business questions answered by the SDI mart (-> query layer)")
bq = pd.DataFrame([{"#": i + 1, "Business question": q["question"], "Query id": q["id"], "Widget": q["widget"],
                    "BI dashboard (Milestone 2)": q["dashboard"]} for i, q in enumerate(QUERIES)])
r = write_df(ws, bq, r, name="bq", widths={"Business question": 70, "Widget": 50, "BI dashboard (Milestone 2)": 46},
             wrap_cols=("Business question",))
r = section(ws, r, "KPI definitions")
kpis = pd.DataFrame([
    ("Forecast bias %", "SUM(forecast - consumed) / SUM(consumed)", "fact_hospital_demand", "month x hospital x medicine", "0 = unbiased; < 0 under-forecast"),
    ("WAPE (forecast error) %", "SUM(|forecast - consumed|) / SUM(consumed)", "fact_hospital_demand", "any roll-up", "Lower is better (~5.6% today)"),
    ("Request-to-receipt fill rate %", "SUM(received) / SUM(requested)", "fact_hospital_demand", "hospital, month", "Target >= 98%"),
    ("Supply gap (packs)", "SUM(requested - received)", "fact_hospital_demand", "hospital, class", "Demand lost in the supply chain"),
    ("Emergency demand %", "SUM(emergency_request_packs) / SUM(consumed)", "fact_hospital_demand", "month, category", "Unplanned demand pressure"),
    ("Unfulfilled demand (packs) / stock-out days", "SUM(unfulfilled_demand_packs), SUM(stockout_days)", "fact_hospital_demand", "hospital x class", "Direct patient-impact signal"),
    ("Service level %", "1 - unfulfilled / (dispensed + unfulfilled)", "fact_hospital_demand", "non-additive: recompute from sums", ""),
    ("Net revenue / gross margin % by channel", "SUM(net_sales_usd), SUM(gross_margin_usd)/SUM(net_sales_usd)", "fact_sales", "year x channel_group", "Margin is flat at 23.4% in every channel"),
    ("YoY growth %", "net(month) / net(month-12) - 1", "agg_sales_daily_channel", "month x channel", "E-commerce fastest"),
    ("Sell-through velocity", "packs sold / active months", "agg_medicine_velocity", "medicine", "Fast / Medium / Slow terciles"),
    ("Unified demand (packs)", "sales packs + hospital ward consumption", "agg_unified_demand_monthly", "month x medicine x region x channel", "The single demand signal for planning"),
    ("E-commerce returned-to-sender %", "RTS orders / e-commerce orders", "agg_sales_monthly", "month", "~25% - fulfilment issue"),
], columns=["KPI", "Formula", "Source table", "Grain", "Interpretation"])
r = write_df(ws, kpis, r, name="kpi", widths={"Formula": 52, "Interpretation": 42})
r = section(ws, r, "How every Phase-1 component serves the problem")
comp = pd.DataFrame([
    ("Oversampling generator", "Produces new months of demand so the forecast-gap story updates live in two refreshes"),
    ("Batch ingestion (hospital ERP, master, supply files)", "Mirrors the real monthly/batch nature of hospital and ERP extracts"),
    ("Streaming ingestion (POS + e-commerce sales)", "Mirrors continuous retail demand; latency matters for near-real-time demand sensing"),
    ("Validation rules", "Bad demand data = bad plans; every rejected row is counted, explained and visible on the dashboards"),
    ("Postgres staging (ODS)", "Lands all three demand streams in one place with full load history and lineage"),
    ("DuckDB star schema (conformed dims)", "Conformed medicine/date/geography/channel make channel demand comparable = the core fix"),
    ("Query layer", "Each business question becomes an optimised, timed SQL query (sub-10 ms on the optimised tier)"),
], columns=["Component", "Role in solving the problem"])
write_df(ws, comp, r, name="comp", widths={"Role in solving the problem": 100})

# ============================================================================== 02 Architecture
ws = sheet("02_Architecture", "Architecture and layer responsibilities",
           "Medallion-style flow: raw Excel/events (bronze) -> Postgres staging (silver) -> DuckDB star schema + aggregates (gold)")
arch = pd.DataFrame([
    ("1 Oversampling", "Python (pandas/numpy)", "sdi/generator/oversample.py · DAG sdi_00", "Base CSV history + earlier refreshes",
     "7 Excel files per refresh (R1, R2)", "ops.generator_log", "Overview"),
    ("2a Batch ingestion", "watchdog + FastAPI + Airflow", "sdi/ingestion/{watcher,api,batch_loader,validation}.py · DAG sdi_01",
     "data/landing/*.xlsx", "staging.* + rejects", "ops.batch_load_log, ops.file_registry", "1 Batch Ingestion"),
    ("2b Stream ingestion", "FastAPI + requests producer", "sdi/ingestion/{stream_producer,api}.py · DAG sdi_02",
     "data/stream_queue/*.xlsx replayed as events", "staging.stg_pharmacy_sales_data", "ops.stream_session/metrics/event_latency", "2 Streaming"),
    ("3 Storage / staging", "PostgreSQL 16", "sql/postgres/*.sql · sdi/ingestion/staging_ddl.py", "validated rows",
     "7 append-only ODS tables with lineage columns", "ops.storage_snapshot/footprint/date_coverage (DAG sdi_05)", "3 Storage"),
    ("4 Transform & model", "DuckDB (SQL) + Python", "sql/duckdb/01_star_schema.sql · sdi/warehouse/transform.py · DAG sdi_03",
     "staging delta above watermark", "2 facts, 10 dims, 4 role-playing views, 5 aggregates", "ops.transform_log, ops.transform_watermark", "Overview"),
    ("5 Query layer", "DuckDB + Postgres", "sdi/query/{catalog,runner}.py · DAG sdi_04 · API GET /query/{id}",
     "SDI mart", "13 business queries x 3 tiers, timed", "ops.query_log", "4 Query Layer"),
    ("Orchestration", "Apache Airflow 3.1", "dags/*.py (7 DAGs)", "-", "-", "ops.pipeline_task_log (callbacks)", "Overview"),
    ("Observability", "Grafana 11", "grafana/provisioning + grafana/dashboards (5 JSON)", "ops.* in Postgres", "-", "-", "all"),
], columns=["Layer", "Technology", "Code / DAG", "Input", "Output", "Metrics captured", "Grafana dashboard"])
r = write_df(ws, arch, 4, name="arch", widths={"Code / DAG": 58, "Output": 40, "Metrics captured": 44}, wrap_cols=("Code / DAG",))
image(ws, FIG / "architecture.png", f"A{r}", width=1100)
r += 32
image(ws, FIG / "dag_flow.png", f"A{r}", width=1100)

# ============================================================================== 03 Source inventory
ws = sheet("03_Source_Inventory", "Source inventory (7 files)", "Grain, keys, size and how each source is used in Phase 1")
inv = []
for k, src in SOURCES.items():
    st = storage[(storage.layer == "staging") & (storage.table_name == src.table)].iloc[-1]
    base_rows = int(batch[(batch.refresh_tag == "BASE") & (batch.source_name == k)].rows_read.sum())
    inv.append(dict(source_file=f"{k}.csv / sheet '{k}'", domain=src.domain, staging_table=f"staging.{src.table}",
                    primary_key=" + ".join(src.pk), columns=len(src.columns), base_rows=base_rows,
                    rows_after_R2=int(st.row_count), coverage_date_column=src.coverage_date, min_date=st.min_date,
                    max_date=st.max_date, used_by_SDI_mart="Yes" if src.sdi_mart or k == "03_manufacturer_delivery_data" else "Staged (Phase 2/3)",
                    refresh_ingest_mode="stream (API)" if src.ingest_mode == "stream" else "batch (watcher + DAG)"))
inv = pd.DataFrame(inv)
r = write_df(ws, inv, 4, name="inv")
note(ws, r, "Grain: 01 one SKU · 02 one manufacturing batch · 03 one shipment line · 04 one lot per day · 05 one sales "
            "transaction · 06 one hospital x medicine x month · 07 one sensor reading. The BASE load reads the provided Excel "
            "workbook (7 sheets); 03 is used for the conformed dim_warehouse.", width_cols=12)

# ============================================================================== 04 Source profile
ws = sheet("04_Source_Profile", "Column-level profile of the provided source data (BASE)",
           "Every column of the 7 CSV files: type, nulls, distinct values, range and examples (profiled with pandas)")
prof = []
for k, src in SOURCES.items():
    df = pd.read_csv(SOURCE_DIR / f"{k}.csv", dtype={"GTIN14": str, "SSCC18": str}, low_memory=False)
    for c, t in src.columns:
        s = df[c]
        num = pd.api.types.is_numeric_dtype(s)
        prof.append(dict(source=k, column=c, staging_column=snake(c), declared_type=t, rows=len(s),
                         nulls=int(s.isna().sum()), null_pct=round(s.isna().mean(), 4), distinct=int(s.nunique()),
                         min=(float(s.min()) if num else str(s.dropna().min())[:30]) if s.notna().any() else None,
                         max=(float(s.max()) if num else str(s.dropna().max())[:30]) if s.notna().any() else None,
                         mean=round(float(s.mean()), 3) if num else None,
                         examples=", ".join(map(str, s.dropna().unique()[:3]))[:80]))
prof = pd.DataFrame(prof)
write_df(ws, prof, 4, name="prof", number_formats={"null_pct": "0.0%"}, widths={"examples": 60})
ws.freeze_panes = "C5"

# ============================================================================== 05 Data quality findings
ws = sheet("05_Data_Quality", "Data-quality findings and how the pipeline handles them",
           "Discovered while profiling and building the pipeline; each has an explicit, documented treatment")
bulk = dkdf("SELECT count(*) n, sum(net_sales_usd) usd FROM fact_sales f JOIN dim_sales_profile p USING (sales_profile_key) "
            "WHERE p.date_quality_flag <> 'Normal'").iloc[0]
rts = dkdf("SELECT sum(rts_txn) a, sum(txn_count) b FROM agg_sales_monthly WHERE channel_group='E-commerce'").iloc[0]
dq = pd.DataFrame([
    ("DQ01", "Excel workbook timestamps are local time (UTC+05:00), not UTC",
     "Every *TimestampUTC value in the workbook is exactly +5 h vs the CSV export (verified on 293,425 IoT and 561 manufacturing rows)",
     "Wrong hour-of-day, wrong date near midnight", "Loader localises naive Excel timestamps as Asia/Karachi and converts to UTC (validation.EXCEL_NAIVE_TZ); generator writes explicit ISO-8601 'Z'"),
    ("DQ02", "Year-start bulk postings in sales",
     f"{int(bulk.n):,} sales ({bulk.n / fs_rows[0]:.1%}) dated 1 Jan 2024/2025 although sold 241-705 days after pharmacy delivery (normal max 240)",
     f"Inflates January demand; ${bulk.usd:,.0f} of revenue sits on two days", "Kept (never delete history) but flagged: dim_sales_profile.date_quality_flag = 'Year-start bulk posting'; trend/velocity/unified-demand queries filter to 'Normal'; generator sizes refreshes on normal daily demand"),
    ("DQ03", "SSCC18 loses precision in the CSV/float form", "867 shipments collapse to 549 distinct SSCC18 values when read as float; CSV also drops the leading zero",
     "Wrong shipment joins via SSCC", "All GS1 identifiers (GTIN14, SSCC18, GLNs) staged as TEXT and zero-padded to their GS1 length; joins use ShipmentID"),
    ("DQ04", "ThresholdBreachFlag also counts shocks", "320 shipment readings flagged as breach with no temperature/humidity breach (shock > 2.5 g)",
     "Over-states cold-chain excursions", "Documented for Phase 3: use Temperature/HumidityBreachFlag for cold-chain KPIs"),
    ("DQ05", "Hospital Pharmacy is a pharmacy type, not a sales channel", "SalesChannel has only 'Physical Pharmacy' and 'Ecommerce'",
     "Spec asks for Retail vs Hospital vs E-commerce", "dim_channel.channel_group derives Retail pharmacy / Hospital pharmacy / E-commerce (+ Hospital ward for the hospital fact)"),
    ("DQ06", "Gross margin identical (23.4%) in every channel", "Prices and COGS are list-based in all channels",
     "No margin story by channel", "Channel dashboard focuses on volume, mix and growth; margin reported as flat"),
    ("DQ07", "E-commerce returned-to-sender rate", f"{int(rts.a):,} of {int(rts.b):,} e-commerce orders ({rts.a / rts.b:.1%}) returned to sender",
     "Possible fulfilment failure", "Q12 tracks the RTS rate per month; secondary finding"),
    ("DQ08", "Hospital demand is not visible in sales", "Hospital-pharmacy sales are ~2% of the packs received by hospitals",
     "Sales alone under-states hospital demand", "fact_hospital_demand is the hospital demand source; agg_unified_demand_monthly combines both"),
    ("DQ09", "Disease activity columns repeat across medicines", "EstimatedDiseaseCases / admissions / encounters are per hospital x disease x month",
     "Summing across medicines multiplies them by 9", "Flagged non-additive across medicines in 10_Fact_Design"),
    ("DQ10", "Low cold-chain coverage", "Only 3 of 63 medicines are cold-chain (Insulin Glargine, Insulin Aspart, Semaglutide)",
     "'Top-10 cold-chain medicines' impossible", "Velocity ranking covers all medicines with a cold-chain flag/filter"),
    ("DQ11", "Injected malformed rows (by design)", "~1% dirty rows per refresh from the generator",
     "Exercises validation", "Rejected with reason codes into rejects.rejected_rows (sheet 06)"),
], columns=["ID", "Finding", "Evidence", "Impact", "Pipeline treatment"])
write_df(ws, dq, 4, name="dq", widths={"Finding": 40, "Evidence": 60, "Impact": 32, "Pipeline treatment": 70},
         wrap_cols=("Finding", "Evidence", "Impact", "Pipeline treatment"))

# ============================================================================== 06 Validation rules & results
ws = sheet("06_Validation", "Validation rules and results", "Shared by batch (Excel) and stream (API) ingestion: sdi/ingestion/validation.py")
rules = [{"source": k, "rule_code": "TYPE_CAST", "kind": "type", "columns": "all non-text", "parameters": "value must cast to declared type"}
         for k in []]
rules = [dict(source="(all)", rule_code="TYPE_CAST", kind="type", columns="all typed columns", parameters="value present but not castable"),
         dict(source="(all)", rule_code="INVALID_DATE", kind="range", columns="all date/timestamp columns", parameters="2020-01-01 .. 2030-12-31"),
         dict(source="(all)", rule_code="DUPLICATE_PK_IN_BATCH", kind="unique", columns="primary key", parameters="first occurrence kept"),
         dict(source="(all)", rule_code="DUPLICATE (PK in staging)", kind="idempotency", columns="primary key", parameters="INSERT .. ON CONFLICT DO NOTHING, counted")]
for k, src in SOURCES.items():
    for ru in src.rules:
        rules.append(dict(source=k, rule_code=ru.code, kind=ru.kind, columns=", ".join(ru.columns), parameters=json.dumps(ru.params) if ru.params else ""))
r = write_df(ws, pd.DataFrame(rules), 4, name="rules", widths={"columns": 60})
r = section(ws, r, "Rejected rows by refresh, source and rule (live)")
start_rej = r
r = write_df(ws, rejects.rename(columns={"rows": "rejected_rows"}), r, name="rej")
end_rej = r - 3
label(ws, f"G{start_rej}", "Total rejected rows"); formula_cell(ws, f"H{start_rej}", f"=SUM(E{start_rej + 1}:E{end_rej + 1})", bold=True)
for i, tag in enumerate(["R1", "R2"]):
    label(ws, f"G{start_rej + 1 + i}", f"Rejected in {tag}")
    formula_cell(ws, f"H{start_rej + 1 + i}", f'=SUMIFS(E{start_rej + 1}:E{end_rej + 1},D{start_rej + 1}:D{end_rej + 1},"{tag}")')
label(ws, f"G{start_rej + 3}", "Stream share of rejects")
formula_cell(ws, f"H{start_rej + 3}", f'=SUMIFS(E{start_rej + 1}:E{end_rej + 1},B{start_rej + 1}:B{end_rej + 1},"stream")/H{start_rej}', "0.0%")
ws.column_dimensions["G"].width = 26
r = section(ws, r, "Sample rejected records (2 per source x rule)")
write_df(ws, reject_samples, r, name="rejs", widths={"record_excerpt": 90, "reason_detail": 40})

# ============================================================================== 07 Bus matrix
ws = sheet("07_Bus_Matrix", "Enterprise bus matrix (conformed dimensions across all phases)",
           "Phase 1 rows are implemented; Phase 2/3 rows show how the conformed dimensions will be reused in the fact constellation")
dims = ["Date", "Medicine", "Geography", "Channel", "Pharmacy", "Hospital", "Promotion", "Sales profile (junk)",
        "Demand profile (junk)", "Warehouse", "Plant", "Carrier", "IoT device", "Batch / lot"]
bus = [("Pharmacy & e-commerce sales", "fact_sales", "1 (built)", "Transaction", [1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 0, 0, 1]),
       ("Hospital demand & consumption", "fact_hospital_demand", "1 (built)", "Periodic snapshot (monthly)", [1, 1, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0]),
       ("Warehouse inventory", "fact_inventory_snapshot", "2", "Periodic snapshot (daily)", [1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1]),
       ("Manufacturing batches", "fact_manufacturing_batch", "2", "Accumulating snapshot", [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1]),
       ("Plant-to-warehouse shipments", "fact_shipment_line", "2/3", "Accumulating snapshot", [1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1]),
       ("IoT telemetry", "fact_sensor_reading / hourly thermal exposure", "3", "Transaction + aggregate", [1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0])]
bm = pd.DataFrame([{"Business process": b[0], "Fact table": b[1], "Phase": b[2], "Fact type": b[3],
                    **{d: ("X" if v else "") for d, v in zip(dims, b[4])}} for b in bus])
r = write_df(ws, bm, 4, name="bus", widths={d: 11 for d in dims})
for rr in range(5, 5 + len(bm)):
    for cc in range(5, 5 + len(dims)):
        c = ws.cell(row=rr, column=cc)
        c.alignment = Alignment(horizontal="center")
        if c.value == "X":
            c.fill = PatternFill("solid", fgColor="C6E0B4" if rr <= 6 else LIGHT)
            c.font = Font(name=FONT, size=9, bold=True)
note(ws, r, "Green = implemented in Phase 1. Conformed dimensions (Date, Medicine, Geography, Channel, Warehouse) are already "
            "built so Phase 2/3 facts plug into the same keys -> drill-across queries for the inventory-management constellation.",
     width_cols=14)

# ============================================================================== 08 Star schema
ws = sheet("08_Star_Schema", "SDI star schema / fact constellation", "Diagram + table list with grain, keys and live row counts")
image(ws, FIG / "star_schema.png", "A4", width=1150)
mart_tables = storage[(storage.layer == "mart")].groupby("table_name").tail(1)
grain = {"fact_sales": "1 row per sales transaction", "fact_hospital_demand": "1 row per hospital x medicine x month",
         "dim_date": "1 row per calendar day 2020-2030 (+ -1 unknown)", "dim_medicine": "1 row per medicine VERSION (SCD2)",
         "dim_hospital": "1 row per hospital VERSION (SCD2)", "dim_pharmacy": "1 row per pharmacy (SCD1)",
         "dim_geography": "1 row per city/state", "dim_warehouse": "1 row per warehouse", "dim_channel": "1 row per channel/pharmacy-type combo",
         "dim_promotion": "1 row per promotion code", "dim_sales_profile": "1 row per observed combination of 8 sale flags (junk)",
         "dim_demand_profile": "1 row per observed combination of 5 demand flags (junk)",
         "agg_sales_monthly": "month x medicine x channel_group x state x date-quality",
         "agg_sales_daily_channel": "day x channel_group x date-quality", "agg_hospital_monthly": "month x hospital x disease category",
         "agg_unified_demand_monthly": "month x medicine x region x channel_group (incl. hospital ward)", "agg_medicine_velocity": "medicine"}
tl = pd.DataFrame([{"table": t.table_name, "kind": t.domain, "grain": grain.get(t.table_name, ""), "rows (after R2)": int(t.row_count),
                    "size_mb": float(t.size_mb or 0)} for t in mart_tables.itertuples()]).sort_values(["kind", "table"], ascending=[False, True])
write_df(ws, tl, 46, name="mart_tables", widths={"grain": 62})

# ============================================================================== 09 Dimension design
ws = sheet("09_Dimension_Design", "Dimension design (column level)",
           "Surrogate keys, natural keys, SCD type per attribute, source lineage and derivation rules")
RENAME = {("dim_medicine", "brand_name"): "01.FictionalBrandName", ("dim_medicine", "effective_from"): "01.EffectiveFromDate",
          ("dim_medicine", "min_remaining_shelf_life_days"): "01.MinimumRemainingShelfLifeDays",
          ("dim_hospital", "state_code"): "06.State", ("dim_hospital", "avg_occupancy_rate_pct"): "06.AverageOccupancyRatePct",
          ("dim_hospital", "effective_from"): "06.DemandMonth (first month the attribute values appear)",
          ("dim_pharmacy", "city"): "05.PharmacyCity", ("dim_pharmacy", "state_code"): "05.PharmacyState",
          ("dim_warehouse", "warehouse_id"): "03.DestinationWarehouseID", ("dim_warehouse", "warehouse_gln"): "03.DestinationWarehouseGLN",
          ("dim_warehouse", "warehouse_name"): "03.DestinationWarehouseName", ("dim_warehouse", "city"): "03.DestinationCity",
          ("dim_warehouse", "state_code"): "03.DestinationState", ("dim_geography", "city"): "05.PharmacyCity / 06.City / 03.DestinationCity",
          ("dim_geography", "state_code"): "05.PharmacyState / 06.State / 03.DestinationState",
          ("dim_channel", "sales_channel"): "05.SalesChannel ('Hospital ERP' for the hospital fact)", ("dim_channel", "pharmacy_type"): "05.PharmacyType",
          ("dim_channel", "source_system"): "05.SourceSystem", ("dim_promotion", "promotion_code"): "05.PromotionCode (NULL -> NO_PROMO)"}
DERIVED = {"storage_condition": "cold_chain_flag='Y' -> '2-8 C' else '15-25 C'", "price_band": "RRP <5 Low, <=20 Mid, >20 High",
           "scd_hash": "md5 of the Type-2 attributes (change detection)", "effective_to": "next version's effective_from - 1 day; 9999-12-31 if current",
           "is_current": "TRUE for the open version", "version_num": "1,2,3... per natural key", "bed_size_band": "<400 / 400-699 / 700+ beds",
           "geography_key": "lookup dim_geography(city, state)", "census_region": "US Census region from state code (static map)",
           "census_division": "US Census division from state code", "state_name": "static state map", "country_code": "'US'",
           "channel_group": "Ecommerce -> E-commerce; Hospital Pharmacy -> Hospital pharmacy; else Retail pharmacy; Hospital ERP -> Hospital ward",
           "channel_family": "Commercial sale / Institutional consumption", "promotion_type": "prefix of code (LOYALTY/SEASON/ONLINE/WELLNESS)",
           "discount_pct": "numeric suffix of the code", "is_promoted": "code <> NO_PROMO",
           "date_quality_flag": "Jan-1 sale AND sale - delivery > 240 days -> 'Year-start bulk posting'",
           "emergency_request_flag": "emergency_request_quantity_packs > 0", "stockout_flag": "stockout_days > 0",
           "unfulfilled_flag": "unfulfilled_demand_packs > 0", "source_batch_id": "staging _batch_id (lineage)", "updated_at": "ETL timestamp"}
DATE_DESC = {"date_key": "yyyymmdd integer surrogate key"}
SRCMAP = {"dim_medicine": "01", "dim_hospital": "06", "dim_pharmacy": "05", "dim_sales_profile": "05", "dim_demand_profile": "06"}
SRC_COLS = {k[:2]: {snake(c): c for c, _ in s.columns} for k, s in SOURCES.items()}
rows = []
for t in ["dim_date", "dim_medicine", "dim_hospital", "dim_pharmacy", "dim_geography", "dim_warehouse", "dim_channel",
          "dim_promotion", "dim_sales_profile", "dim_demand_profile"]:
    for col, typ, *_ in duck.execute(f"DESCRIBE {t}").fetchall():
        if col.endswith("_key") and col == duck.execute(f"DESCRIBE {t}").fetchone()[0]:
            role = "Surrogate key (PK)"
        elif col.endswith("_key"):
            role = "Foreign key (outrigger)"
        elif col in ("medicine_id", "hospital_id", "pharmacy_id", "warehouse_id"):
            role = "Natural / business key"
        elif col in ("scd_hash", "effective_from", "effective_to", "is_current", "version_num", "source_batch_id", "updated_at"):
            role = "SCD / audit"
        else:
            role = "Attribute"
        scd = ""
        if t == "dim_medicine" and role == "Attribute":
            scd = "Type 2" if col in MEDICINE_T2 else "Type 1" if col in MEDICINE_T1 else "Type 1 (static)"
        elif t == "dim_hospital" and role == "Attribute":
            scd = "Type 2" if col in HOSPITAL_T2 or col == "avg_occupancy_rate_pct" else "Type 1"
        elif t == "dim_pharmacy" and role == "Attribute":
            scd = "Type 1"
        elif t in ("dim_sales_profile", "dim_demand_profile") and role == "Attribute":
            scd = "Junk (insert-only)"
        src = RENAME.get((t, col))
        if not src and t in SRCMAP and col in SRC_COLS[SRCMAP[t]]:
            src = f"{SRCMAP[t]}.{SRC_COLS[SRCMAP[t]][col]}"
        deriv = DERIVED.get(col, "")
        if t == "dim_date" and role != "Surrogate key (PK)":
            deriv, src = "generated calendar 2020-01-01..2030-12-31", "generated"
        rows.append(dict(table=t, column=col, data_type=typ, key_role=role, scd_type=scd, source=src or ("ETL" if deriv else ""),
                         derivation=deriv or DATE_DESC.get(col, "")))
dd = pd.DataFrame(rows)
r = write_df(ws, dd, 4, name="dimdesign", widths={"derivation": 70, "source": 48})
ws.freeze_panes = "C5"
note(ws, r, "Every dimension has an 'Unknown' member with key -1, so a failed lookup never drops a fact row (orphan count is "
            "reported by the reconciliation step: 0 in all runs).", width_cols=7)

# ============================================================================== 10 Fact design
ws = sheet("10_Fact_Design", "Fact table design (column level)", "Grain, fact type, measure additivity and lineage")
r = kv(ws, 4, [("fact_sales", "Transaction fact · grain = one pharmacy/e-commerce sales transaction · PK = sales_transaction_id (degenerate)"),
               ("fact_hospital_demand", "Periodic snapshot fact (monthly) · grain = one hospital x medicine x month · PK = hospital_demand_record_id (degenerate)"),
               ("Fact constellation", "Both facts share dim_date (role-playing), dim_medicine, dim_geography and dim_channel -> drill-across "
                                      "(agg_unified_demand_monthly, query Q09)")])
FS_SRC = {"sales_transaction_id": "SalesTransactionID", "sale_date_key": "SalesDate", "delivery_date_key": "PharmacyDeliveryDate",
          "expiry_date_key": "BatchExpiryDate", "medicine_key": "MedicineID + SalesDate (SCD2 version valid on the sale date)",
          "pharmacy_key": "PharmacyID", "geography_key": "PharmacyCity + PharmacyState (via dim_pharmacy)", "channel_key": "SalesChannel + PharmacyType",
          "promotion_key": "PromotionCode", "sales_profile_key": "CustomerSegment, PayerType, PaymentMethod, PrescriptionRequiredFlag, EcommerceDeliveryStatus, ReturnFlag, SaleStatus + date-quality rule",
          "warehouse_key": "SourceWarehouseID", "pharmacy_order_id": "PharmacyOrderID", "pharmacy_delivery_id": "PharmacyDeliveryID",
          "prescription_id": "PrescriptionID", "warehouse_lot_id": "WarehouseLotID", "production_batch_id": "ProductionBatchID",
          "sales_timestamp_utc": "SalesTimestampUTC (normalised to UTC)", "sale_hour": "hour(SalesTimestampUTC)",
          "quantity_sold_packs": "QuantitySoldPacks", "unit_retail_price_usd": "UnitRetailPriceUSD", "gross_sales_usd": "GrossSalesAmountUSD",
          "discount_usd": "DiscountAmountUSD", "net_sales_usd": "NetSalesAmountUSD", "sales_tax_usd": "SalesTaxUSD", "cogs_usd": "CostOfGoodsSoldUSD",
          "gross_margin_usd": "GrossMarginUSD", "returned_qty_packs": "ReturnedQuantityPacks", "days_to_expiry_at_sale": "DaysToExpiryAtSale",
          "delivery_quantity_packs": "DeliveryQuantityPacks", "delivery_otif_flag": "DeliveryOTIFFlag", "ingest_mode": "staging _ingest_mode",
          "refresh_tag": "staging _refresh_tag", "etl_batch_id": "staging _batch_id", "etl_loaded_at": "now() at load"}
FH_SRC = {"hospital_demand_record_id": "HospitalDemandRecordID", "demand_month_key": "DemandMonth", "hospital_key": "HospitalID + DemandMonth (SCD2 version)",
          "medicine_key": "MedicineID + DemandMonth (SCD2 version)", "geography_key": "City + State (via dim_hospital)", "channel_key": "constant 'Hospital ward'",
          "demand_profile_key": "MedicationRequestStatus, FormularyStatus + emergency/stockout/unfulfilled flags",
          "forecast_error_packs": "ForecastDemandPacks - ConsumedQuantityPacks", "abs_forecast_error_packs": "|ForecastDemandPacks - ConsumedQuantityPacks|",
          "supply_gap_packs": "RequestedQuantityPacks - ReceivedQuantityPacks", "requested_qty_packs": "RequestedQuantityPacks",
          "approved_qty_packs": "ApprovedQuantityPacks", "received_qty_packs": "ReceivedQuantityPacks", "emergency_request_packs": "EmergencyRequestQuantityPacks",
          "dispensed_qty_packs": "DispensedQuantityPacks", "consumed_qty_packs": "ConsumedQuantityPacks", "wastage_qty_packs": "WastageQuantityPacks",
          "avg_lead_time_days": "AverageLeadTimeDays", "refresh_tag": "staging _refresh_tag", "etl_batch_id": "staging _batch_id", "etl_loaded_at": "now() at load"}
ADD = {"non": ["unit_retail_price_usd", "days_to_expiry_at_sale", "delivery_quantity_packs", "service_level_pct", "avg_lead_time_days",
               "demand_trend_index", "estimated_disease_cases", "disease_related_admissions", "outpatient_encounters"],
       "semi": ["opening_stock_packs", "closing_stock_packs"]}
frows = []
for t, smap, pre in [("fact_sales", FS_SRC, "05"), ("fact_hospital_demand", FH_SRC, "06")]:
    for col, typ, *_ in duck.execute(f"DESCRIBE {t}").fetchall():
        if col.endswith("_key"):
            role = "Foreign key"
        elif col.endswith("_id"):
            role = "Degenerate dimension"
        elif col in ("ingest_mode", "refresh_tag", "etl_batch_id", "etl_loaded_at", "sales_timestamp_utc", "sale_hour", "delivery_otif_flag"):
            role = "Audit / descriptive"
        else:
            role = "Measure"
        add = ""
        if role == "Measure":
            add = "Non-additive" if col in ADD["non"] else "Semi-additive (not across time)" if col in ADD["semi"] else "Additive"
        if col in ("estimated_disease_cases", "disease_related_admissions", "outpatient_encounters"):
            add = "Non-additive across medicines (repeats per disease)"
        src = smap.get(col) or next((sc for sc in SRC_COLS[pre].values() if snake(sc) == col), "")
        frows.append(dict(fact=t, column=col, data_type=typ, role=role, additivity=add, source=f"{pre}.{src}" if src and "staging" not in src and "now()" not in src and "constant" not in src else src))
r = write_df(ws, pd.DataFrame(frows), r, name="factdesign", widths={"source": 80, "additivity": 40})
ws.freeze_panes = "C5"

# ============================================================================== 11 Source to target
ws = sheet("11_Source_to_Target", "Source-to-target mapping (column lineage)",
           "Excel column -> Postgres staging column (type cast + validation) -> SDI mart column")
target_of = {}
for _, rr in pd.DataFrame(frows).iterrows():
    for part in str(rr["source"]).replace("+", ",").split(","):
        part = part.strip().split(" ")[0]
        if "." in part:
            target_of.setdefault(part, []).append(f"{rr['fact']}.{rr['column']}")
for _, rr in dd.iterrows():
    for part in str(rr["source"]).replace("/", ",").split(","):
        part = part.strip().split(" ")[0]
        if "." in part:
            target_of.setdefault(part, []).append(f"{rr['table']}.{rr['column']}")
stt = []
for k, src in SOURCES.items():
    rule_cols = {}
    for ru in src.rules:
        for c in ru.columns:
            rule_cols.setdefault(c, []).append(ru.code)
    for c, t in src.columns:
        stt.append(dict(source_file=k, source_column=c, staging_column=f"staging.{src.table}.{snake(c)}",
                        staging_type={"text": "TEXT", "bigint": "BIGINT", "double": "DOUBLE PRECISION", "date": "DATE",
                                      "timestamp": "TIMESTAMPTZ (UTC)"}[t],
                        validation=", ".join(rule_cols.get(c, []) + (["INVALID_DATE"] if t in ("date", "timestamp") else []) +
                                             (["PK"] if c in src.pk else [])),
                        sdi_mart_target="; ".join(sorted(set(target_of.get(f"{k[:2]}.{c}", [])))) or
                                        ("(staged for Phase 2/3)" if not src.sdi_mart else "(not modelled)")))
r = write_df(ws, pd.DataFrame(stt), 4, name="stt", widths={"sdi_mart_target": 70, "staging_column": 55})
ws.freeze_panes = "C5"

# ============================================================================== 12 SCD history
ws = sheet("12_SCD_History", "Slowly Changing Dimensions — policy and live history",
           "R1 and R2 deliberately changed prices, a brand name, launched medicines and expanded a hospital to prove SCD handling")
scd_pol = pd.DataFrame([("dim_medicine", "Type 2", ", ".join(MEDICINE_T2), "close old version (effective_to = new from - 1), insert version n+1"),
                        ("dim_medicine", "Type 1", ", ".join(MEDICINE_T1), "overwrite on ALL versions (corrections, renames)"),
                        ("dim_hospital", "Type 2", ", ".join(HOSPITAL_T2), "new version effective from the demand month the change appears"),
                        ("dim_pharmacy", "Type 1", "name, type, city, state, GLN", "overwrite"),
                        ("dim_sales_profile / dim_demand_profile", "Junk (Type 0)", "flag combinations", "insert unseen combinations only")],
                       columns=["dimension", "SCD type", "attributes", "mechanism"])
r = write_df(ws, scd_pol, 4, name="scdpol", widths={"attributes": 80, "mechanism": 60})
r = section(ws, r, "Scenario injected by the generator")
sc = pd.DataFrame([{"refresh": t, "price changes (factor)": json.dumps(s["price_changes"]), "brand renames (Type 1)": json.dumps(s["brand_renames"]),
                    "new medicines": ", ".join(f"{m['MedicineID']} {m['GenericName']}" for m in s["new_medicines"]),
                    "hospital changes": json.dumps(s["hospital_changes"])} for t, s in SCENARIOS.items()])
r = write_df(ws, sc, r, name="scen")
r = section(ws, r, "dim_medicine — every medicine with more than one version, the renamed brand and the launches (live)")
med = dkdf("""SELECT medicine_key, medicine_id, generic_name, brand_name, unit_manufacturing_cost_usd, wholesale_price_usd,
                     recommended_retail_price_usd, product_status, effective_from, effective_to, is_current, version_num, source_batch_id
              FROM dim_medicine WHERE medicine_id IN (SELECT medicine_id FROM dim_medicine GROUP BY 1 HAVING count(*) > 1)
                 OR medicine_id IN ('MED045','MED064','MED065') ORDER BY medicine_id, version_num""")
r = write_df(ws, med, r, name="medscd")
r = section(ws, r, "dim_hospital — HOSP003 capacity expansion (live)")
hosp = dkdf("SELECT hospital_key, hospital_id, hospital_name, licensed_beds, icu_beds, bed_size_band, effective_from, effective_to, "
            "is_current, version_num FROM dim_hospital WHERE hospital_id = 'HOSP003' ORDER BY version_num")
r = write_df(ws, hosp, r, name="hospscd")
r = section(ws, r, "Proof: facts point at the version valid on the transaction date (Amlodipine, MED010)")
proof = dkdf("""SELECT m.medicine_id, m.version_num, m.recommended_retail_price_usd AS rrp_in_dimension,
                       min(d.full_date) AS first_sale, max(d.full_date) AS last_sale, count(*) AS sales,
                       round(avg(f.unit_retail_price_usd),2) AS avg_price_on_fact
                FROM fact_sales f JOIN dim_medicine m USING (medicine_key) JOIN dim_date d ON d.date_key = f.sale_date_key
                WHERE m.medicine_id IN ('MED010','MED024','MED007') AND f.sale_date_key >= 20251101 GROUP BY 1,2,3 ORDER BY 1,2""")
write_df(ws, proof, r, name="scdproof")

# ============================================================================== 13 Junk & role-playing
ws = sheet("13_Junk_RolePlaying", "Junk dimensions and role-playing dimensions",
           "Low-cardinality flags collapsed into junk dimensions; one date dimension playing four roles")
rp = pd.DataFrame([("dim_sale_date", "fact_sales.sale_date_key", "When the sale happened"),
                   ("dim_delivery_date", "fact_sales.delivery_date_key", "When the pharmacy replenishment delivery arrived"),
                   ("dim_expiry_date", "fact_sales.expiry_date_key", "Expiry of the batch that was sold (shelf-life analysis)"),
                   ("dim_demand_month", "fact_hospital_demand.demand_month_key", "Hospital demand month (month-start rows only)")],
                  columns=["role-playing view (of dim_date)", "fact column", "meaning"])
r = write_df(ws, rp, 4, name="roles", widths={"meaning": 60})
r = section(ws, r, "dim_sales_profile (junk) — attribute cardinality")
jcard = dkdf("""SELECT 'customer_segment' attribute, count(DISTINCT customer_segment) distinct_values FROM dim_sales_profile UNION ALL
                SELECT 'payer_type', count(DISTINCT payer_type) FROM dim_sales_profile UNION ALL
                SELECT 'payment_method', count(DISTINCT payment_method) FROM dim_sales_profile UNION ALL
                SELECT 'prescription_required_flag', count(DISTINCT prescription_required_flag) FROM dim_sales_profile UNION ALL
                SELECT 'ecommerce_delivery_status', count(DISTINCT ecommerce_delivery_status) FROM dim_sales_profile UNION ALL
                SELECT 'return_flag', count(DISTINCT return_flag) FROM dim_sales_profile UNION ALL
                SELECT 'sale_status', count(DISTINCT sale_status) FROM dim_sales_profile UNION ALL
                SELECT 'date_quality_flag', count(DISTINCT date_quality_flag) FROM dim_sales_profile""")
jstart = r
r = write_df(ws, jcard, r, name="junkcard")
label(ws, f"D{jstart}", "Theoretical combinations"); formula_cell(ws, f"E{jstart}", f"=PRODUCT(B{jstart + 1}:B{jstart + len(jcard)})")
label(ws, f"D{jstart + 1}", "Observed combinations (rows)")
ws[f"E{jstart + 1}"] = int(duck.execute("SELECT count(*) FROM dim_sales_profile WHERE sales_profile_key > 0").fetchone()[0])
ws[f"E{jstart + 1}"].font = Font(name=FONT, size=9, color="0000FF"); ws[f"E{jstart + 1}"].number_format = "#,##0"
label(ws, f"D{jstart + 2}", "Fill ratio"); formula_cell(ws, f"E{jstart + 2}", f"=E{jstart + 1}/E{jstart}", "0.0%")
ws.column_dimensions["D"].width = 30
r = section(ws, r, "Top 15 junk combinations by number of sales")
top = dkdf("""SELECT p.*, count(*) AS sales FROM fact_sales f JOIN dim_sales_profile p USING (sales_profile_key)
              GROUP BY ALL ORDER BY sales DESC LIMIT 15""")
r = write_df(ws, top, r, name="junktop")
r = section(ws, r, "dim_demand_profile (junk) — all rows with hospital-month counts")
dp = dkdf("""SELECT p.*, count(f.hospital_demand_record_id) AS hospital_months FROM dim_demand_profile p
             LEFT JOIN fact_hospital_demand f USING (demand_profile_key) GROUP BY ALL ORDER BY hospital_months DESC""")
write_df(ws, dp, r, name="demprof")

# ============================================================================== 14 Aggregates
ws = sheet("14_Aggregates", "Pre-aggregated summary tables (materialised-view equivalents)",
           "DuckDB has no materialised views: Airflow rebuilds these after every load (transform.build_aggregates)")
uses = {}
for q in QUERIES:
    for a in AGGREGATES_SQL:
        if a in q["sql"]["star_optimized"]:
            uses.setdefault(a, []).append(q["id"])
ag = pd.DataFrame([{"aggregate": a, "grain": grain.get(a, ""), "rows (after R2)": int(mart_tables[mart_tables.table_name == a].row_count.iloc[0]),
                    "used by queries": ", ".join(uses.get(a, [])), "SQL": " ".join(sql.split())} for a, sql in AGGREGATES_SQL.items()])
astart = 4
r = write_df(ws, ag, astart, name="aggs", widths={"SQL": 110, "grain": 50}, wrap_cols=("SQL",))
label(ws, f"A{r}", "fact_sales rows"); ws[f"B{r}"] = int(fs_rows[0]); ws[f"B{r}"].font = Font(name=FONT, size=9, color="0000FF"); ws[f"B{r}"].number_format = "#,##0"
label(ws, f"A{r + 1}", "agg_sales_monthly compression (fact rows / agg rows)")
formula_cell(ws, f"B{r + 1}", f"=B{r}/C{astart + 1}", "0.0x")

# ============================================================================== 15 DAG catalog
ws = sheet("15_DAG_Catalog", "Airflow DAG catalog (modular task design)",
           "7 DAGs in dags/, each task calls one function of the sdi package; default retries=2, retry_delay=1 min, timeout=30 min")
dagcat = [
    ("sdi_00_data_generator", "manual (param refresh_tag)", "generate_all_sources", "@task", "-", "Oversample all 7 files for the target month"),
    ("sdi_00_data_generator", "", "validate_generated", "@task", "generate_all_sources", "Files exist, rows > 0"),
    ("sdi_00_data_generator", "", "log_generator_stats", "@task", "validate_generated", "-> ops.generator_log"),
    ("sdi_00_data_generator", "", "publish_to_landing_and_stream_queue", "@task", "log_generator_stats", "atomic copy: batch files -> landing/, sales -> stream_queue/"),
    ("sdi_01_batch_ingestion", "*/2 * * * * + watcher trigger", "discover_pending_files", "@task.short_circuit", "-", "ops.file_registry PENDING + safety scan of landing/"),
    ("sdi_01_batch_ingestion", "", "load_master_first", "@task", "discover_pending_files", "medicine master first (FK validation needs it)"),
    ("sdi_01_batch_ingestion", "", "load_file", "@task + .expand() (dynamic mapping)", "load_master_first", "one mapped task per Excel file; read->validate->COPY->log"),
    ("sdi_01_batch_ingestion", "", "summarize", "@task (none_failed)", "load_file", "rows loaded / rejected"),
    ("sdi_01_batch_ingestion", "", "trigger_transform", "TriggerDagRunOperator", "summarize", "kick sdi_03"),
    ("sdi_02_stream_ingestion", "*/5 * * * *", "check_stream_queue", "@task.short_circuit", "-", "skip if nothing queued"),
    ("sdi_02_stream_ingestion", "", "wait_for_ingestion_api", "PythonSensor (reschedule)", "check_stream_queue", "GET /health"),
    ("sdi_02_stream_ingestion", "", "stream_sales_events", "@task", "wait_for_ingestion_api", "producer: 50-event micro-batches at 400 ev/s"),
    ("sdi_02_stream_ingestion", "", "validate_session", "@task", "stream_sales_events", "every event accounted for"),
    ("sdi_02_stream_ingestion", "", "trigger_transform", "TriggerDagRunOperator", "validate_session", "kick sdi_03"),
    ("sdi_03_transform", "@hourly + triggered", "init_mart", "@task", "-", "idempotent DDL, dim_date, unknown members"),
    ("sdi_03_transform", "", "extract_delta", "@task.short_circuit", "init_mart", "staging rows > watermark -> CSV hand-off; skip if none"),
    ("sdi_03_transform", "", "load_dimensions", "@task", "extract_delta", "geography, medicine SCD2/1, hospital SCD2, pharmacy SCD1, warehouse, channel, promotion, junk"),
    ("sdi_03_transform", "", "load_fact_sales", "@task", "load_dimensions", "key lookups (SCD2 as-of date), anti-join on txn id"),
    ("sdi_03_transform", "", "load_fact_hospital_demand", "@task", "load_fact_sales", "key lookups, derived measures"),
    ("sdi_03_transform", "", "build_aggregates", "@task", "load_fact_hospital_demand", "rebuild 5 summary tables"),
    ("sdi_03_transform", "", "reconcile_and_commit", "@task", "build_aggregates", "staging = mart checks; advance watermark only on success"),
    ("sdi_03_transform", "", "trigger_query_layer", "TriggerDagRunOperator", "reconcile_and_commit", "kick sdi_04"),
    ("sdi_04_query_layer", "06:00 daily + triggered", "apply_optimizations", "@task", "-", "re-cluster fact, ART indexes, partitioned Parquet"),
    ("sdi_04_query_layer", "", "run_benchmark", "@task", "apply_optimizations", "13 queries x tiers x 3 repeats; fails if tiers disagree"),
    ("sdi_04_query_layer", "", "dashboard_refresh_simulation", "@task", "run_benchmark", "every widget once (optimised tier)"),
    ("sdi_04_query_layer", "", "adhoc_query_simulation", "@task", "dashboard_refresh_simulation", "6 analyst queries (2 deliberately wrong)"),
    ("sdi_04_query_layer", "", "trigger_storage_metrics", "TriggerDagRunOperator", "adhoc_query_simulation", "kick sdi_05"),
    ("sdi_05_storage_metrics", "@hourly + triggered", "collect_storage_metrics", "@task", "-", "row counts, MB, date coverage"),
    ("sdi_05_storage_metrics", "", "check_date_coverage", "@task", "collect_storage_metrics", "fail if any month 2023-01..latest is missing"),
    ("sdi_99_refresh_cycle", "manual (BASE/R1/R2)", "base_or_refresh -> generate | stage_base_workbook -> batch -> stream -> transform -> query",
     "@task.branch + TriggerDagRunOperator(wait_for_completion)", "-", "one-click end-to-end refresh for the demo"),
]
r = write_df(ws, pd.DataFrame(dagcat, columns=["dag_id", "schedule", "task_id", "operator", "upstream", "purpose"]), 4, name="dags",
             widths={"purpose": 70, "task_id": 44})
r = section(ws, r, "Airflow task outcomes captured by callbacks (live, ops.pipeline_task_log)")
tsum = tasklog.groupby(["dag_id", "task_id", "state"]).agg(runs=("run_id", "count"), avg_duration_s=("duration_s", "mean"),
                                                           max_duration_s=("duration_s", "max")).reset_index()
tsum["avg_duration_s"] = tsum.avg_duration_s.astype(float).round(2); tsum["max_duration_s"] = tsum.max_duration_s.astype(float).round(2)
write_df(ws, tsum, r, name="tasksum")

# ============================================================================== 16 Oversampling
ws = sheet("16_Oversampling", "Oversampling (data generation) methodology and statistics",
           "sdi/generator/oversample.py — one synthetic month per refresh for all 7 source files, lineage-preserving")
method = pd.DataFrame([
    ("01 master", "Full snapshot; SCD scenario applied (price x factor, renames, launches)", "63 (R1) / 65 (R2) rows; unchanged rows are skipped as duplicates by staging"),
    ("02 manufacturing", "Bootstrap last-12-month batches; dates shifted into the month; quantities x noise; costs/yield/expiry recomputed", "avg monthly batches x growth"),
    ("03 delivery", "Per released batch: template batch's shipment lines, rescaled to released qty; OTIF/received recomputed; GS1 SSCC with check digit", "follows batches"),
    ("04 warehouse", "Daily lot simulation: continuing lots + lots for deliveries arriving in the month; FEFO/cycle-count/stock flags; opening = previous closing", "lots x days on hand"),
    ("05 sales", "Bootstrap last-12-month NORMAL sales (bulk postings excluded); each sale re-linked to a lot on hand with expiry > sale date; prices from the CURRENT master (SCD flows through)", "avg normal daily sales (last 92 days) x days x growth"),
    ("06 hospital demand", "Same calendar month last year x growth x bed-capacity factor; forecast keeps a ~1% under-forecast bias; opening = previous closing; unfulfilled when stock runs out", "12 hospitals x active medicines"),
    ("07 IoT", "Warehouse zones every 3 h per device (template = same device last month + noise); shipment loggers copied from a same-mode template; one injected cold-room excursion", "24 zone devices x 8/day + shipments"),
], columns=["source", "method", "volume rule"])
r = write_df(ws, method, 4, name="genmethod", widths={"method": 100, "volume rule": 60}, wrap_cols=("method",))
r = kv(ws, r, [("Growth factor (YoY)", f"{YOY_GROWTH} (cross-checked: hospital consumption +3.5%/yr, sales +8%/yr in the history)"),
               ("Dirty-row rates", json.dumps(DIRTY_RATE)), ("Seeds", "numpy default_rng(yyyymm of the refresh month) -> reproducible"),
               ("Why one month per refresh", "Two refreshes over different time periods (Jan-2026, Feb-2026) as the spec requires; "
                                             "~25k rows per refresh is large enough to move every dashboard but small enough to keep the history dominant "
                                             "(+2.4% per refresh) so trends stay realistic")])
r = section(ws, r, "Rows generated per refresh (live ops.generator_log)")
g = gen.copy()
base_rows = batch[batch.refresh_tag == "BASE"].groupby("source_name").rows_read.sum()
g["base_rows"] = g.source_name.map(base_rows).astype("Int64")
g = g[["refresh_tag", "source_name", "target_month", "base_rows", "rows_generated", "dirty_rows"]]
gstart = r
r = write_df(ws, g, r, name="genstats")
n = len(g)
ws.cell(row=gstart, column=7, value="increment vs BASE").font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
ws.cell(row=gstart, column=7).fill = PatternFill("solid", fgColor=BLUE)
ws.cell(row=gstart, column=8, value="dirty %").font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
ws.cell(row=gstart, column=8).fill = PatternFill("solid", fgColor=BLUE)
for i in range(1, n + 1):
    formula_cell(ws, f"G{gstart + i}", f"=IFERROR(E{gstart + i}/D{gstart + i},0)", "0.00%")
    formula_cell(ws, f"H{gstart + i}", f"=IFERROR(F{gstart + i}/E{gstart + i},0)", "0.00%")
ws.column_dimensions["G"].width = 16; ws.column_dimensions["H"].width = 10
tr = gstart + n + 2
for i, tag in enumerate(["R1", "R2"]):
    label(ws, f"A{tr + i}", f"Total rows generated {tag}")
    formula_cell(ws, f"B{tr + i}", f'=SUMIFS(E{gstart + 1}:E{gstart + n},A{gstart + 1}:A{gstart + n},"{tag}")', bold=True)
    label(ws, f"C{tr + i}", f"Dirty rows {tag}")
    formula_cell(ws, f"D{tr + i}", f'=SUMIFS(F{gstart + 1}:F{gstart + n},A{gstart + 1}:A{gstart + n},"{tag}")')
label(ws, f"A{tr + 2}", "Grand total generated"); formula_cell(ws, f"B{tr + 2}", f"=B{tr}+B{tr + 1}", bold=True)

# ============================================================================== 17 Batch ingestion stats
ws = sheet("17_Batch_Ingestion", "Batch ingestion statistics (ops.batch_load_log)",
           "Batch Load Duration = end - start per sheet (read + validate + insert); Rows processed = len(df); Success = no unhandled exception")
bl = batch[["load_id", "refresh_tag", "source_name", "file_name", "file_size_mb", "start_ts", "end_ts", "duration_s", "read_s",
            "validate_s", "insert_s", "rows_read", "rows_loaded", "rows_rejected", "rows_duplicate", "rows_per_sec", "status", "dag_run_id"]].copy()
for c in ["file_size_mb", "duration_s", "read_s", "validate_s", "insert_s", "rows_per_sec"]:
    bl[c] = bl[c].astype(float)
bstart = 4
r = write_df(ws, bl, bstart, name="batchlog", widths={"file_name": 50, "dag_run_id": 30})
nb = len(bl)
end = bstart + nb
r = section(ws, r, "Summary by refresh (formulas over the log above)")
hdr = ["refresh", "sheets", "rows read", "rows loaded", "rejected", "duplicates", "total seconds", "success rate", "rows / second"]
for j, h in enumerate(hdr):
    label(ws, f"{get_column_letter(1 + j)}{r}", h)
for i, tag in enumerate(["BASE", "R1", "R2"], start=1):
    rr = r + i
    ws[f"A{rr}"] = tag; ws[f"A{rr}"].font = Font(name=FONT, size=9, bold=True)
    rng = lambda col: f"{col}{bstart + 1}:{col}{end}"
    formula_cell(ws, f"B{rr}", f'=COUNTIFS({rng("B")},A{rr})')
    formula_cell(ws, f"C{rr}", f'=SUMIFS({rng("L")},{rng("B")},A{rr})')
    formula_cell(ws, f"D{rr}", f'=SUMIFS({rng("M")},{rng("B")},A{rr})')
    formula_cell(ws, f"E{rr}", f'=SUMIFS({rng("N")},{rng("B")},A{rr})')
    formula_cell(ws, f"F{rr}", f'=SUMIFS({rng("O")},{rng("B")},A{rr})')
    formula_cell(ws, f"G{rr}", f'=SUMIFS({rng("H")},{rng("B")},A{rr})', "#,##0.0")
    formula_cell(ws, f"H{rr}", f'=COUNTIFS({rng("B")},A{rr},{rng("Q")},"SUCCESS")/B{rr}', "0.0%")
    formula_cell(ws, f"I{rr}", f"=IFERROR(C{rr}/G{rr},0)", "#,##0")
chart = BarChart(); chart.type = "bar"; chart.style = 10
chart.title = "BASE load: duration per Excel sheet (s)"; chart.y_axis.title = "seconds"
data = Reference(ws, min_col=8, min_row=bstart, max_row=bstart + 7)
cats = Reference(ws, min_col=3, min_row=bstart + 1, max_row=bstart + 7)
chart.add_data(data, titles_from_data=True); chart.set_categories(cats)
chart.height, chart.width = 8, 16; chart.legend = None; chart.roundedCorners = False
chart.x_axis.delete = False; chart.y_axis.delete = False; chart.title.overlay = False
ws.add_chart(chart, f"K{r}")
r += 20
r = section(ws, r, "File registry (watcher -> API -> DAG)")
write_df(ws, registry, r, name="registry", widths={"file_path": 70})

# ============================================================================== 18 Streaming stats
ws = sheet("18_Streaming", "Streaming ingestion statistics",
           "Throughput = ingested / stream duration · Latency = DB commit - event creation · Malformed = rejected in the API's validation")
ss = stream_s.copy()
for c in ["duration_s", "throughput_eps", "avg_latency_ms", "p95_latency_ms"]:
    ss[c] = ss[c].astype(float)
sstart = 4
r = write_df(ws, ss, sstart, name="sessions")
ns = len(ss)
r = section(ws, r, "Totals (formulas)")
label(ws, f"A{r}", "Events sent"); formula_cell(ws, f"B{r}", f"=SUM(H{sstart + 1}:H{sstart + ns})")
label(ws, f"A{r + 1}", "Events ingested"); formula_cell(ws, f"B{r + 1}", f"=SUM(I{sstart + 1}:I{sstart + ns})")
label(ws, f"A{r + 2}", "Malformed + duplicate"); formula_cell(ws, f"B{r + 2}", f"=SUM(J{sstart + 1}:K{sstart + ns})")
label(ws, f"A{r + 3}", "Accounted for (ingested + rejected) / sent"); formula_cell(ws, f"B{r + 3}", f"=(B{r + 1}+B{r + 2})/B{r}", "0.00%")
label(ws, f"A{r + 4}", "Malformed rate"); formula_cell(ws, f"B{r + 4}", f"=SUM(J{sstart + 1}:J{sstart + ns})/B{r}", "0.00%")
label(ws, f"A{r + 5}", "Overall throughput (events/s)"); formula_cell(ws, f"B{r + 5}", f"=B{r + 1}/SUM(G{sstart + 1}:G{sstart + ns})", "#,##0.0")
r += 7
r = section(ws, r, "Per-second metrics (ops.stream_metrics)")
sm = stream_m.copy()
sm["avg_latency_ms"] = sm.avg_latency_ms.astype(float); sm["max_latency_ms"] = sm.max_latency_ms.astype(float)
write_df(ws, sm, r, name="persec")

# ============================================================================== 19 Storage
ws = sheet("19_Storage", "Storage & staging metrics", "Row counts, physical size and date coverage after every refresh (ops.storage_*)")
latest = storage.groupby(["layer", "table_name"]).tail(1).copy()
latest["size_mb"] = latest.size_mb.astype(float)
latest = latest[["layer", "table_name", "domain", "row_count", "size_mb", "min_date", "max_date", "span_days", "distinct_months"]]
lstart = 4
r = write_df(ws, latest, lstart, name="storlatest")
nl = len(latest)
r = section(ws, r, "Footprint (formulas)")
label(ws, f"A{r}", "Staging rows"); formula_cell(ws, f"B{r}", f'=SUMIFS(D{lstart + 1}:D{lstart + nl},A{lstart + 1}:A{lstart + nl},"staging")')
label(ws, f"A{r + 1}", "Staging MB (tables)"); formula_cell(ws, f"B{r + 1}", f'=SUMIFS(E{lstart + 1}:E{lstart + nl},A{lstart + 1}:A{lstart + nl},"staging")', "#,##0.0")
label(ws, f"A{r + 2}", "Mart MB (tables)"); formula_cell(ws, f"B{r + 2}", f'=SUMIFS(E{lstart + 1}:E{lstart + nl},A{lstart + 1}:A{lstart + nl},"mart")', "#,##0.0")
label(ws, f"A{r + 3}", "Sales: staging MB / fact MB (row vs column store)")
srow = latest.reset_index(drop=True)
i_stg = lstart + 1 + srow.index[srow.table_name == "stg_pharmacy_sales_data"][0]
i_fact = lstart + 1 + srow.index[srow.table_name == "fact_sales"][0]
formula_cell(ws, f"B{r + 3}", f"=E{i_stg}/E{i_fact}", "0.0x", bold=True)
ws.column_dimensions["A"].width = 44
r += 5
r = section(ws, r, "Database / file footprint over time (pg_database_size, os.path.getsize)")
fp = footprint.copy()
for c in ["pg_database_mb", "duckdb_file_mb", "parquet_dir_mb"]:
    fp[c] = fp[c].astype(float)
r = write_df(ws, fp.drop(columns=["pg_datadir_mb"]), r, name="footprint")
r = section(ws, r, "Rows per staging table after each refresh")
grow = storage[storage.layer == "staging"].groupby(["refresh_tag", "table_name"]).row_count.last().unstack(0).reset_index()
r = write_df(ws, grow, r, name="growth")
r = section(ws, r, "Date coverage: rows per year-month per source (all months 2023-01 .. 2026-02 present)")
cov = coverage.pivot_table(index="year_month", columns="source_name", values="row_count", aggfunc="sum").reset_index()
cov.columns = [str(c) for c in cov.columns]
write_df(ws, cov, r, name="coverage")

# ============================================================================== 20 Transform log
ws = sheet("20_Transform_Log", "Transformation log and reconciliation", "Every transform step, rows in/inserted/updated, duration and the reconciliation verdicts")
tl2 = tlog.drop(columns=["transform_id"]).copy()
tl2["duration_s"] = tl2.duration_s.astype(float)
r = write_df(ws, tl2, 4, name="tlog", widths={"details": 100}, wrap_cols=("details",))
r = section(ws, r, "Reconciliation checks per refresh (staging vs mart)")
rec = []
for _, row in tlog[tlog.step == "reconcile"].iterrows():
    d = json.loads(row.details)
    for k, (a, b) in d["checks"].items():
        rec.append(dict(refresh=row.refresh_tag, check=k, staging=a, mart=b))
rec = pd.DataFrame(rec)
rstart = r
r = write_df(ws, rec, r, name="recon")
ws.cell(row=rstart, column=5, value="difference").font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
ws.cell(row=rstart, column=5).fill = PatternFill("solid", fgColor=BLUE)
ws.cell(row=rstart, column=6, value="status").font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
ws.cell(row=rstart, column=6).fill = PatternFill("solid", fgColor=BLUE)
for i in range(1, len(rec) + 1):
    formula_cell(ws, f"E{rstart + i}", f"=D{rstart + i}-C{rstart + i}", "#,##0.00")
    formula_cell(ws, f"F{rstart + i}", f'=IF(ABS(E{rstart + i})<0.05,"PASS","FAIL")', "@", bold=True)

# ============================================================================== 21 Query catalog
ws = sheet("21_Query_Catalog", "Analytical query catalog, optimisation and benchmark",
           "13 business queries x 3 tiers (+ lake_parquet for Q11); identical results verified automatically on every run")
qb2 = qlog[(qlog.caller == "benchmark") & (qlog.status == "SUCCESS")].copy()
qb2["exec_ms"] = qb2.exec_ms.astype(float)
avg = qb2.groupby(["query_id", "tier"]).exec_ms.mean().unstack()
qc = pd.DataFrame([{"query_id": q["id"], "widget": q["widget"], "BI dashboard": q["dashboard"], "business question": q["question"],
                    "optimisation applied": q["optimization"],
                    "staging_ms": round(avg.loc[q["id"], "oltp_staging"], 2), "star_ms": round(avg.loc[q["id"], "star_schema"], 2),
                    "optimised_ms": round(avg.loc[q["id"], "star_optimized"], 2)} for q in QUERIES])
qstart = 4
r = write_df(ws, qc, qstart, name="qcat", widths={"widget": 40, "BI dashboard": 34, "business question": 50, "optimisation applied": 55},
             wrap_cols=("business question", "optimisation applied", "widget"))
nq = len(qc)
for j, hname in enumerate(["speed-up vs staging", "speed-up vs star"]):
    c = ws.cell(row=qstart, column=9 + j, value=hname)
    c.font = Font(name=FONT, size=9, bold=True, color="FFFFFF"); c.fill = PatternFill("solid", fgColor=BLUE)
for i in range(1, nq + 1):
    formula_cell(ws, f"I{qstart + i}", f"=F{qstart + i}/H{qstart + i}", "0.0x")
    formula_cell(ws, f"J{qstart + i}", f"=G{qstart + i}/H{qstart + i}", "0.0x")
ws.column_dimensions["I"].width = 14; ws.column_dimensions["J"].width = 14
label(ws, f"E{qstart + nq + 1}", "Average (all queries)")
for col in "FGH":
    formula_cell(ws, f"{col}{qstart + nq + 1}", f"=AVERAGE({col}{qstart + 1}:{col}{qstart + nq})", "#,##0.0", bold=True)
formula_cell(ws, f"I{qstart + nq + 1}", f"=F{qstart + nq + 1}/H{qstart + nq + 1}", "0.0x", bold=True)
chart = BarChart(); chart.type = "col"; chart.style = 10; chart.grouping = "clustered"
chart.title = "Average execution time per widget by tier (ms)"
data = Reference(ws, min_col=6, max_col=8, min_row=qstart, max_row=qstart + nq)
cats = Reference(ws, min_col=1, min_row=qstart + 1, max_row=qstart + nq)
chart.add_data(data, titles_from_data=True); chart.set_categories(cats)
chart.height, chart.width = 9, 24; chart.roundedCorners = False
chart.x_axis.delete = False; chart.y_axis.delete = False; chart.title.overlay = False; chart.legend.overlay = False
chart.y_axis.title = "ms"; chart.y_axis.scaling.logBase = 10
ws.add_chart(chart, f"A{qstart + nq + 3}")
r = qstart + nq + 23
r = section(ws, r, "Full SQL per tier")
sqlrows = []
for q in QUERIES:
    for tier, sql in q["sql"].items():
        sqlrows.append(dict(query_id=q["id"], tier=tier, engine="postgres" if tier == "oltp_staging" else "duckdb",
                            sql="\n".join(l.rstrip() for l in sql.strip("\n").splitlines())))
write_df(ws, pd.DataFrame(sqlrows), r, name="qsql", widths={"sql": 130}, wrap_cols=("sql",))
r = r + len(sqlrows) + 3
r = section(ws, r, "Ad-hoc analyst queries (error-rate simulation; ADH03 and ADH05 are deliberately wrong)")
write_df(ws, pd.DataFrame(ADHOC, columns=["query_id", "engine", "sql"]), r, name="adhoc", widths={"sql": 100})

# ============================================================================== 22 Query log
ws = sheet("22_Query_Log", "Query execution log (every timed execution)",
           "Query Execution Time (ms), Daily Active Query Count, Query Error Rate are computed from this log")
ql = qlog.drop(columns=["business_question"]).copy()
ql["exec_ms"] = ql.exec_ms.astype(float)
qls = 6
label(ws, "A4", "Total executions"); formula_cell(ws, "B4", f"=COUNTA(F{qls + 1}:F{qls + len(ql)})")
label(ws, "C4", "Failed"); formula_cell(ws, "D4", f'=COUNTIFS(L{qls + 1}:L{qls + len(ql)},"FAILED")')
label(ws, "E4", "Error rate"); formula_cell(ws, "F4", "=D4/B4", "0.00%", bold=True)
write_df(ws, ql, qls, name="qlog", widths={"error_message": 60, "widget_name": 40})
ws.freeze_panes = f"A{qls + 1}"

# ============================================================================== 23 Query results
ws = sheet("23_Query_Results", "Query results from the SDI mart (optimised tier, after R2)",
           "What the business sees: the answer to each business question (feeds the Milestone-2 BI dashboards)")
r = 4
for q in QUERIES:
    df = duck.execute(q["sql"]["star_optimized"]).df()
    r = section(ws, r, f"{q['id']} — {q['widget']}")
    r = note(ws, r, q["question"], width_cols=8)
    r = write_df(ws, df.head(60), r, name=f"res_{q['id']}")

# ============================================================================== 24 Mart profile
ws = sheet("24_Mart_Profile", "SDI mart profile and measure reconciliation",
           "Row counts by refresh / ingest mode and measure totals; staging totals are inputs, differences by formula")
mp = dkdf("""SELECT refresh_tag, ingest_mode, count(*) AS rows, sum(quantity_sold_packs) AS packs, round(sum(net_sales_usd),2) AS net_usd,
                    round(sum(gross_margin_usd),2) AS margin_usd, min(sale_date_key) AS first_date_key, max(sale_date_key) AS last_date_key
             FROM fact_sales GROUP BY 1,2 ORDER BY 1""")
r = write_df(ws, mp, 4, name="mpsales")
mh = dkdf("""SELECT refresh_tag, count(*) AS rows, sum(consumed_qty_packs) AS consumed, sum(forecast_demand_packs) AS forecast,
                    sum(unfulfilled_demand_packs) AS unfulfilled, round(100.0*sum(forecast_error_packs)/sum(consumed_qty_packs),2) AS bias_pct
             FROM fact_hospital_demand GROUP BY 1 ORDER BY 1""")
r = write_df(ws, mh, r, name="mphosp")
r = section(ws, r, "Measure reconciliation: Postgres staging vs DuckDB mart (all refreshes)")
stg = pgdf("""SELECT count(*) c, sum(quantity_sold_packs) q, round(sum(net_sales_amount_usd)::numeric,2) n,
                     round(sum(gross_margin_usd)::numeric,2) m FROM staging.stg_pharmacy_sales_data""").iloc[0]
mrt = dkdf("SELECT count(*) c, sum(quantity_sold_packs) q, round(sum(net_sales_usd),2) n, round(sum(gross_margin_usd),2) m FROM fact_sales").iloc[0]
label(ws, f"A{r}", "Measure"); label(ws, f"B{r}", "Staging (input)"); label(ws, f"C{r}", "Mart (input)"); label(ws, f"D{r}", "Difference")
for i, (k, a, b) in enumerate([("sales rows", stg.c, mrt.c), ("packs", stg.q, mrt.q), ("net sales USD", stg.n, mrt.n),
                               ("gross margin USD", stg.m, mrt.m)], start=1):
    ws[f"A{r + i}"] = k; ws[f"A{r + i}"].font = Font(name=FONT, size=9)
    for col, v in (("B", a), ("C", b)):
        ws[f"{col}{r + i}"] = float(v); ws[f"{col}{r + i}"].font = Font(name=FONT, size=9, color="0000FF")
        ws[f"{col}{r + i}"].number_format = "#,##0.00"
    formula_cell(ws, f"D{r + i}", f"=C{r + i}-B{r + i}", "#,##0.00", bold=True)
ws.column_dimensions["A"].width = 24

# ============================================================================== 25 Assumptions
ws = sheet("25_Assumptions", "Assumptions and design decisions", "Explicit assumptions behind the Phase-1 implementation")
ass = pd.DataFrame([
    ("A01", "Data", "The provided workbook is the system of record for the BASE load; the CSVs are used only as generator history (faster to read)."),
    ("A02", "Data", "Workbook timestamps are UTC+05:00 local time (verified +5 h on every row) and are converted to UTC on load."),
    ("A03", "Data", "Year-start bulk postings (Jan-1 sales > 240 days after delivery) are a source-system artefact; kept and flagged, excluded from trend/velocity KPIs."),
    ("A04", "Scope", "Phase-1 mart uses 01 (medicine), 05 (sales) and 06 (hospital demand); 03 feeds the conformed dim_warehouse. 02, 04, 07 are ingested and stored now (storage dashboard covers all 7 domains) and modelled in Phases 2/3."),
    ("A05", "Channels", "Retail = physical chain + independent pharmacies; Hospital = physical hospital pharmacies; E-commerce = Ecommerce channel; Hospital ward = hospital ERP consumption (institutional, not a sale)."),
    ("A06", "Ingestion", "Refresh increments of sales arrive as a stream (POS/e-commerce are event-driven); all other increments arrive as Excel batch files."),
    ("A07", "Ingestion", "Streaming is simulated at 400 events/s in 50-event micro-batches; latency = DB commit - producer creation time (same clock)."),
    ("A08", "Ingestion", "The file-watcher calls the ingestion API, which registers the file and (optionally) triggers Airflow via REST; the batch DAG also polls every 2 min as a safety net."),
    ("A09", "Storage", "Staging is append-only; primary keys make re-delivery idempotent (duplicates counted, not loaded)."),
    ("A10", "Storage", "Postgres footprint = pg_database_size / pg_total_relation_size (the data directory is not readable from the Airflow container); DuckDB footprint = os.path.getsize of the .duckdb file + storage blocks per table."),
    ("A11", "Model", "Medicine prices/status are Type 2 (history matters for revenue analysis); names/class are Type 1 (corrections). Hospital capacity is Type 2 (demand depends on beds)."),
    ("A12", "Model", "Disease-activity measures are non-additive across medicines (they repeat per disease)."),
    ("A13", "Model", "DuckDB has no materialised views, so summary tables are rebuilt by Airflow after every load (full rebuild is < 0.5 s at this size)."),
    ("A14", "Model", "Foreign keys are enforced by the ETL (lookups with unknown-member -1) plus reconciliation, not by DuckDB constraints (DuckDB FK constraints block updates of SCD rows)."),
    ("A15", "Query", "The OLTP-staging tier represents 'before the warehouse' (analysts querying the ODS directly); all tiers must return identical results or the DAG fails."),
    ("A16", "Query", "Ad-hoc analyst traffic is simulated (6 queries per run, 2 deliberately wrong) so the error-rate metric is exercised."),
    ("A17", "Generator", "Growth 5%/yr, ~1% dirty rows, seeds per month -> reproducible refreshes; refresh months Jan-2026 (R1) and Feb-2026 (R2)."),
    ("A18", "Platform", "Grafana reads only the Postgres ops schema; dashboards are provisioned from JSON so they are version-controlled."),
], columns=["ID", "Area", "Assumption / decision"])
write_df(ws, ass, 4, name="assume", widths={"Assumption / decision": 140}, wrap_cols=("Assumption / decision",))

# ============================================================================== TOC on cover
ws = wb["00_Cover"]
r = toc_row
label(ws, f"A{r}", "Sheet"); label(ws, f"B{r}", "Contents")
for i, (name, title, purpose) in enumerate(TOC, start=1):
    c = ws.cell(row=r + i, column=1, value=name)
    c.hyperlink = f"#'{name}'!A1"
    c.font = Font(name=FONT, size=9, color="0563C1", underline="single")
    d = ws.cell(row=r + i, column=2, value=f"{title} — {purpose}")
    d.font = Font(name=FONT, size=9)
    d.alignment = Alignment(wrap_text=True)
ws.column_dimensions["B"].width = 110

for w in wb.worksheets:
    w.sheet_properties.tabColor = {"0": NAVY, "1": BLUE, "2": "548235"}.get(w.title[0], "C55A11")
OUT.parent.mkdir(parents=True, exist_ok=True)
wb.save(OUT)
print("saved", OUT, "sheets:", len(wb.sheetnames))
