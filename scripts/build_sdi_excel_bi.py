"""Builds docs/SDI.Excel.BI.xlsx - the complete SDI BI-layer workbook - from the live state
(Postgres sdi_bi serving schema + ops logs, DuckDB mart, Superset API, dashboards-as-code spec, screenshots).
Run after a full BASE + R1 + R2 cycle (with sdi_06_bi_refresh) and the per-refresh screenshots:
    python scripts/build_sdi_excel_bi.py"""
import sys
import json
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.drawing.image import Image as XLImage
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN, DUCKDB_PATH
from sdi.bi.publish import BI_TABLES, STAR_TABLES, KPIS
from sdi.bi import superset as ss

OUT = ROOT / "docs" / "SDI.Excel.BI.xlsx"
SHOTS = ROOT / "docs" / "bi_screenshots"
FIG = ROOT / "docs" / "figures"
FONT = "Arial"
NAVY, BLUE, LIGHT, YELLOW, GREEN = "1F3864", "2F5597", "DDEBF7", "FFF2CC", "E2EFDA"
thin = Side(style="thin", color="BFBFBF")
BORDER = Border(left=thin, right=thin, top=thin, bottom=thin)
pg = psycopg2.connect(PG_DSN)
pgdf = lambda s: pd.read_sql(s, pg)
VIZ_NAME = {"big_number_total": "KPI card", "echarts_timeseries_line": "Line chart", "echarts_timeseries_bar": "Bar chart",
            "echarts_area": "Stacked area chart", "pie": "Donut chart", "funnel": "Funnel", "heatmap_v2": "Heatmap",
            "table": "Table", "treemap_v2": "Treemap", "bubble_v2": "Bubble chart", "sunburst_v2": "Sunburst",
            "pivot_table_v2": "Pivot table"}

wb = Workbook()
wb.remove(wb.active)
TOC = []
_t = [0]


def sheet(name, title, purpose):
    ws = wb.create_sheet(name)
    ws.sheet_view.showGridLines = False
    ws["A1"] = title
    ws["A1"].font = Font(name=FONT, size=14, bold=True, color=NAVY)
    ws["A2"] = purpose
    ws["A2"].font = Font(name=FONT, size=9, italic=True, color="595959")
    TOC.append((name, title, purpose))
    return ws


def section(ws, row, text, col=1):
    ws.cell(row=row, column=col, value=text).font = Font(name=FONT, size=11, bold=True, color=BLUE)
    return row + 1


def write_df(ws, df, row, col=1, name=None, widths=None, wrap=(), fmts=None, max_w=60):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]) and getattr(df[c].dt, "tz", None) is not None:
            df[c] = df[c].dt.tz_localize(None)
    for j, h in enumerate(df.columns):
        c = ws.cell(row=row, column=col + j, value=str(h))
        c.font = Font(name=FONT, size=9, bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor=BLUE)
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = BORDER
    for i, r in enumerate(df.itertuples(index=False), start=1):
        for j, v in enumerate(r):
            try:
                if v is not None and not isinstance(v, (str, list, dict)) and pd.isna(v):
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
            c.alignment = Alignment(vertical="top", wrap_text=df.columns[j] in wrap)
            if isinstance(v, float):
                c.number_format = "#,##0.00"
            elif isinstance(v, int) and not isinstance(v, bool):
                c.number_format = "#,##0"
            elif isinstance(v, dt.datetime):
                c.number_format = "yyyy-mm-dd hh:mm"
            elif isinstance(v, dt.date):
                c.number_format = "yyyy-mm-dd"
            if fmts and df.columns[j] in fmts:
                c.number_format = fmts[df.columns[j]]
    if len(df) and name:
        _t[0] += 1
        ref = f"{get_column_letter(col)}{row}:{get_column_letter(col + len(df.columns) - 1)}{row + len(df)}"
        t = Table(displayName=f"t{_t[0]}_{name}", ref=ref)
        t.tableStyleInfo = TableStyleInfo(name="TableStyleLight9", showRowStripes=True)
        ws.add_table(t)
    for j, h in enumerate(df.columns):
        L = get_column_letter(col + j)
        w = (widths or {}).get(h) or min(max([len(str(h))] + [len(str(x)) for x in df[h].head(200)]) + 2, max_w)
        ws.column_dimensions[L].width = max(ws.column_dimensions[L].width or 0, w, 8)
    return row + len(df) + 2


def note(ws, row, text, col=1, cols=10, bold=False, size=9, color="404040", italic=True):
    c = ws.cell(row=row, column=col, value=text)
    c.font = Font(name=FONT, size=size, italic=italic, bold=bold, color=color)
    c.alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=row, start_column=col, end_row=row, end_column=col + cols - 1)
    ws.row_dimensions[row].height = max(15, 13 * (1 + len(text) // (cols * 13)))
    return row + 1


def label(ws, ref, text, fill=LIGHT, bold=True):
    ws[ref] = text
    ws[ref].font = Font(name=FONT, size=9, bold=bold)
    ws[ref].fill = PatternFill("solid", fgColor=fill)
    ws[ref].border = BORDER
    ws[ref].alignment = Alignment(wrap_text=True, vertical="top")


def fcell(ws, ref, formula, fmt="#,##0", bold=False):
    ws[ref] = formula
    ws[ref].font = Font(name=FONT, size=9, bold=bold)
    ws[ref].number_format = fmt
    ws[ref].border = BORDER


def image(ws, path, anchor, width=None):
    if Path(path).exists():
        img = XLImage(str(path))
        if width:
            r = width / img.width
            img.width, img.height = width, int(img.height * r)
        ws.add_image(img, anchor)
        return img.height
    return 0


def chart_style(ch):
    ch.roundedCorners = False
    if ch.legend is not None:
        ch.legend.overlay = False
    if ch.title is not None:
        ch.title.overlay = False
    ch.x_axis.delete = False
    ch.y_axis.delete = False


# ============================================================================================ data pulls
refresh_log = pgdf("SELECT refresh_tag, published_at, data_as_of, latest_demand_month, fact_sales_rows, fact_hospital_rows, "
                   "medicines, tables_published, rows_published, publish_seconds FROM ops.bi_refresh_log ORDER BY refresh_id")
kpi_hist = pgdf("SELECT * FROM sdi_bi.bi_kpi_history")
kpi_piv = kpi_hist.pivot_table(index=["kpi_order", "dashboard", "kpi_name", "unit"], columns="refresh_tag",
                               values="kpi_value", aggfunc="max").reset_index().sort_values("kpi_order")
tags = [t for t in ["BASE", "R1", "R2"] if t in kpi_piv.columns]
bi_q = pgdf("SELECT executed_at, refresh_tag, query_id, widget_name, business_question AS dashboard, exec_ms, rows_returned, "
            "status FROM ops.query_log WHERE caller='bi_dashboard' ORDER BY executed_at")
publog = pgdf("SELECT * FROM ops.bi_publish_log ORDER BY published_at, table_name")
api = ss.Superset()
charts_meta = {}
for d in ss.DASHBOARDS:
    did = api.find("dashboard", "slug", d["slug"])
    for c in api.call("GET", f"dashboard/{did}/charts")["result"]:
        charts_meta.setdefault(c["slice_name"], {"id": c["id"], "dashboards": []})["dashboards"].append(d["title"])
latest = refresh_log.iloc[-1]

# ============================================================================================ 00 Cover
ws = sheet("00_Cover", "SDI.Excel.BI — Business Intelligence layer of the Sales & Demand Intelligence (SDI) mart",
           "Phase 1 · Milestone 2 (complete pipeline with BI and refreshing) · Data Analytics & Warehousing, Fall 2026")
r = 4
gm_file = ROOT / "docs" / "group_members.csv"
gm = pd.read_csv(gm_file, dtype=str).fillna("") if gm_file.exists() else pd.DataFrame(columns=["name", "student_id"])
gm = gm[gm["name"].str.strip() != ""] if len(gm) else gm
r = section(ws, r, "Group members")
r = write_df(ws, gm.rename(columns={"name": "Name", "student_id": "ERP / Student ID"})[["Name", "ERP / Student ID"]]
             if len(gm) else pd.DataFrame({"Name": [""], "ERP / Student ID": [""]}), r, name="group")
r = section(ws, r, "What this milestone adds")
for t in ["BI tool: Apache Superset 4.1 (open source, Docker) — 4 interactive dashboards, 47 charts, native filters, "
          "cross-filtering, drill-to-detail, 30-second auto-refresh, results never cached.",
          "BI serving layer: Airflow publishes the DuckDB gold mart into the Postgres schema sdi_bi (built aside, swapped "
          "atomically) read by a least-privilege bi_reader role — dashboards never see a half-loaded state.",
          "Automated refresh: DAG sdi_06_bi_refresh (publish → KPI snapshot → dashboards-as-code sync → timed warm-up) runs after "
          "every transform and as the last step of sdi_99_refresh_cycle.",
          f"Refreshed twice in the run captured here: BASE (data to {refresh_log.iloc[0].data_as_of}) → R1 → R2 (data to "
          f"{latest.data_as_of}); every KPI per refresh is in 08_Refresh_Evidence and screenshots in 15–17."]:
    r = note(ws, r, "• " + t, cols=8, italic=False)
r += 1
r = section(ws, r, "Headline KPIs after the latest refresh (" + str(latest.refresh_tag) + ")")
head = kpi_piv[["dashboard", "kpi_name", "unit", tags[-1]]].rename(columns={tags[-1]: "value"})
r = write_df(ws, head, r, name="headline", fmts={"value": "#,##0.00"})
r = section(ws, r, "Contents")
toc_row = r
ws.column_dimensions["A"].width = 30
ws.column_dimensions["B"].width = 50

# ============================================================================================ 01 Problem -> BI
ws = sheet("01_Problem_to_BI", "From the business problem to the BI dashboards",
           "Every dashboard visual answers a business question that comes from the SDI problem statement")
r = note(ws, 4, "PROBLEM: planners cannot see true medicine demand. POS sales, e-commerce orders and hospital ERP demand arrive "
                "in three systems at three grains, so hospital forecasts under-shoot consumption, demand is lost between "
                "request and receipt, and production is planned on incomplete signals. The SDI BI layer turns the unified "
                "mart into four dashboards that make each part of this problem visible, measurable and actionable — and they "
                "refresh automatically whenever new data arrives.", cols=8, italic=False, bold=True, color=NAVY, size=10)
r += 1
qmap = pd.DataFrame([
    ("Are hospital forecasts keeping up with consumption?", "BI-1", "Forecast vs actual (line), Forecast bias by month (bar), KPI: Forecast bias, WAPE",
     "Under-forecast in every category (bias ≈ −1%) → raise forecasts / safety stock"),
    ("Where is hospital demand lost?", "BI-1", "Funnel requested→approved→received→consumed, Supply gap by hospital, KPI: Fill rate, Supply gap",
     "~7.3% of requested packs never arrive; gap grows each refresh"),
    ("Which hospitals / therapeutic classes need action first?", "BI-1", "Heatmap WAPE hospital×disease, Hotspot table",
     "Pacific University Hospital dominates unfulfilled & emergency demand"),
    ("How do the channels compare on revenue, growth, margin?", "BI-2", "Stacked area by channel, Donut split, Revenue by year & channel, Margin % by channel & year",
     "Retail ≈60% of revenue; margin flat 23.4% everywhere → growth is a volume story"),
    ("Do promotions pay off? Is e-commerce fulfilment working?", "BI-2", "Promotion effectiveness table, RTS rate by month",
     "Promotions give 5–10% discount without basket lift; ~25% of e-com orders returned to sender"),
    ("Which medicines move fast or slow — esp. cold chain?", "BI-3", "Top-10 fast/slow bars, Treemap by class, Velocity vs revenue bubble, League table",
     "3 cold-chain SKUs = 2% of packs, 14% of revenue, slowest movers → small frequent batches"),
    ("What is total demand across ALL channels incl. hospital wards?", "BI-4", "Unified monthly demand by channel, Sunburst region→channel, Demand vs supply",
     "Hospital wards ≈92% of packs — sales data alone hides most demand"),
    ("Did the warehouse and dashboards change with new data?", "BI-4", "Refresh status banner (all dashboards), KPI-by-refresh pivot, Refresh log, Latest transactions",
     "BASE → R1 → R2 visible on screen and in 08_Refresh_Evidence"),
], columns=["Business question", "Dashboard", "Visuals that answer it", "What the data says / action"])
r = write_df(ws, qmap, r, name="qmap", widths={"Business question": 46, "Visuals that answer it": 60,
                                                 "What the data says / action": 60}, wrap=tuple(qmap.columns))
r = section(ws, r, "How each dashboard solves part of the problem")
for d in ss.DASHBOARDS:
    r = note(ws, r, d["title"], cols=8, italic=False, bold=True, color=BLUE)
    r = note(ws, r, d["story"].replace("### ", "").replace("**", "").replace("\n\n", "\n"), cols=8, italic=False)
    ws.row_dimensions[r - 1].height = 120

# ============================================================================================ 02 Architecture
ws = sheet("02_BI_Architecture", "BI architecture and refresh flow",
           "Medallion: bronze (landing Excel / events) → silver (Postgres staging) → gold (DuckDB mart) → serving (Postgres sdi_bi) → Superset")
arch = pd.DataFrame([
    ("Bronze", "data/landing, stream API", "Raw Excel files and streamed sales events", "sdi_01 / sdi_02"),
    ("Silver", "Postgres staging.*", "Validated, typed, append-only ODS with lineage", "sdi_01 / sdi_02"),
    ("Gold", "DuckDB sdi_mart.duckdb (+ Parquet)", "Star schema / fact constellation + aggregates", "sdi_03 / sdi_04"),
    ("Serving", "Postgres sdi_bi", f"{len(STAR_TABLES)} star tables + {len(BI_TABLES)} BI tables + 2 views; atomic schema swap; bi_reader role", "sdi_06 publish_bi_mart"),
    ("BI", "Apache Superset 4.1", f"{len(ss.DATASETS)} datasets, {len(charts_meta)} charts, {len(ss.DASHBOARDS)} dashboards, native filters, cross-filters", "sdi_06 sync_superset"),
    ("Observability", "Grafana + ops.*", "BI chart fetch times, refresh log, KPI snapshots", "sdi_06 warm_bi_dashboards"),
], columns=["Layer", "Technology / location", "Content", "Written by (Airflow)"])
r = write_df(ws, arch, 4, name="arch", widths={"Content": 70})
r = section(ws, r, "Refresh flow (DAG sdi_06_bi_refresh)")
flow = pd.DataFrame([
    (1, "publish_bi_mart", "@task.short_circuit", "Builds BI tables in DuckDB under the mart lock, COPYs star + BI tables into sdi_bi_new, indexes, swaps schemas in one transaction, grants bi_reader, logs ops.bi_refresh_log / ops.bi_publish_log. Skips on a fresh stack."),
    (2, "snapshot_bi_kpis", "@task", f"Computes {len(KPIS)} headline KPIs on the serving layer → ops.bi_kpi_snapshot (before/after evidence)."),
    (3, "wait_for_superset", "PythonSensor (reschedule)", "Waits for Superset /health."),
    (4, "sync_superset", "@task", "Dashboards as code (sdi/bi/superset.py): database, datasets (+ column refresh, saved metrics), charts with stored query contexts, dashboard layouts, native filters. Idempotent."),
    (5, "warm_bi_dashboards", "@task", "Fetches every dashboard chart through Superset with force=true (live query), timed → ops.query_log (caller bi_dashboard). Fails the DAG if any chart errors."),
], columns=["step", "task", "operator", "what it does"])
r = write_df(ws, flow, r, name="flow", widths={"what it does": 110}, wrap=("what it does",))
r = section(ws, r, "Triggers & freshness")
for t in ["sdi_03_transform → trigger_bi_refresh after every successful reconcile (automatic chain).",
          "sdi_99_refresh_cycle → bi_refresh as the last step (wait_for_completion) — one click refreshes data, DWH and dashboards.",
          "Schedule @hourly as a safety net; max_active_runs = 1; retries = 2.",
          "Superset: NullCache for query results + dashboard auto-refresh every 30 s → a refresh appears on screen within 30 s.",
          "Security: Superset connects as bi_reader (USAGE + SELECT on sdi_bi only, plus the two ops tables behind its views)."]:
    r = note(ws, r, "• " + t, cols=8, italic=False)
image(ws, FIG / "bi_architecture.png", f"A{r + 1}", width=1100)

# ============================================================================================ 03 Dashboard catalog
ws = sheet("03_Dashboard_Catalog", "Dashboard catalogue — every visual", "Generated from the dashboards-as-code spec (sdi/bi/superset.py) + live Superset chart ids")
rows = []
for d in ss.DASHBOARDS:
    for ri, row in enumerate(d["rows"]):
        for item, w, h in row:
            if item == "MARKDOWN":
                rows.append(dict(dashboard=d["title"].split(" · ")[1], row=ri + 1, visual="Story panel (markdown)",
                                 type="Markdown", dataset="", metrics="", dimensions="", filters="", width_of_12=w,
                                 chart_id=None))
                continue
            fd = item["fd"]
            mets = fd.get("metrics") or [fd.get(k) for k in ("metric", "x", "y", "size") if fd.get(k)]
            dims = ([fd["x_axis"]] if fd.get("x_axis") else []) + (fd.get("groupby") if isinstance(fd.get("groupby"), list)
                                                                   else ([fd["groupby"]] if fd.get("groupby") else [])) \
                   + fd.get("columns", []) + fd.get("all_columns", []) + fd.get("groupbyRows", []) + fd.get("groupbyColumns", []) \
                   + ([fd["entity"], fd["series"]] if fd.get("entity") else [])
            flt = "; ".join(f"{f['subject']} {f['operator']} {f['comparator']}" for f in fd.get("adhoc_filters", []))
            rows.append(dict(dashboard=d["title"].split(" · ")[1], row=ri + 1, visual=item["name"],
                             type=VIZ_NAME.get(item["viz"], item["viz"]), dataset=f"sdi_bi.{item['ds']}",
                             metrics=", ".join(mets), dimensions=", ".join(dict.fromkeys(dims)), filters=flt,
                             width_of_12=w, chart_id=charts_meta.get(item["name"], {}).get("id")))
cat = pd.DataFrame(rows)
r = write_df(ws, cat, 4, name="catalog", widths={"visual": 52, "metrics": 40, "dimensions": 40, "filters": 30})
r = section(ws, r, "Visual types used (formula counts from the catalogue above)")
types = sorted(cat.type.unique())
label(ws, f"A{r}", "Visual type"); label(ws, f"B{r}", "Count")
for i, t in enumerate(types, start=1):
    ws.cell(row=r + i, column=1, value=t).font = Font(name=FONT, size=9)
    fcell(ws, f"B{r + i}", f'=COUNTIF($D$5:$D${4 + len(cat)},A{r + i})')
fcell(ws, f"B{r + len(types) + 1}", f"=SUM(B{r + 1}:B{r + len(types)})", bold=True)
label(ws, f"A{r + len(types) + 1}", "Total visuals")
ws.freeze_panes = "D5"

# ============================================================================================ 04 KPI dictionary
ws = sheet("04_KPI_Dictionary", "KPI & metric dictionary", "Saved metrics of the Superset semantic layer + the KPIs snapshotted after every refresh")
md = pd.DataFrame([dict(dataset=f"sdi_bi.{t}", metric=m["metric_name"], label=m["verbose_name"], sql_expression=m["expression"],
                        format=m.get("d3format", ""), meaning=m.get("description", ""))
                   for t, spec in ss.DATASETS.items() for m in spec["metrics"]])
r = write_df(ws, md, 4, name="metrics", widths={"sql_expression": 80, "meaning": 40})
r = section(ws, r, "Headline KPIs snapshotted after each refresh (ops.bi_kpi_snapshot)")
kd = pd.DataFrame([dict(order=o, dashboard=d, kpi=n, unit=u, sql=s.format(s="sdi_bi").strip()) for d, o, n, u, s in KPIS])
write_df(ws, kd, r, name="kpidefs", widths={"sql": 110})

# ============================================================================================ 05 Semantic layer
ws = sheet("05_Semantic_Layer", "Semantic layer — Superset datasets", "One dataset per BI table; time column, grain, rows and full column list")
grain = {"bi_hospital_demand": "hospital x medicine x month", "bi_supply_funnel": "hospital x medicine x month x stage",
         "bi_sales_monthly": "month x medicine x channel x state x promo type x segment x date-quality x ingest mode x refresh",
         "bi_medicine_velocity": "medicine", "bi_unified_demand": "month x medicine x region x channel",
         "bi_demand_supply_monthly": "month x region x disease", "bi_recent_sales": "newest 1,000 transactions",
         "bi_refresh_history": "one BI publish", "bi_kpi_history": "refresh x KPI"}
cnt = {t: pgdf(f"SELECT count(*) n FROM sdi_bi.{t}").n[0] for t in ss.DATASETS}
dsrows = pd.DataFrame([dict(dataset=t, grain=grain.get(t, ""), rows=int(cnt[t]), time_column=spec["dttm"] or "(none)",
                            saved_metrics=len(spec["metrics"]), description=spec["desc"]) for t, spec in ss.DATASETS.items()])
r = write_df(ws, dsrows, 4, name="datasets", widths={"grain": 60, "description": 60})
r = section(ws, r, "Columns (information_schema)")
cols = pgdf("SELECT table_name AS dataset, ordinal_position AS pos, column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema='sdi_bi' AND table_name IN ({','.join(repr(t) for t in ss.DATASETS)}) ORDER BY 1,2")
write_df(ws, cols, r, name="dscols")

# ============================================================================================ 06 Serving schema
ws = sheet("06_Serving_Schema", "Serving schema sdi_bi (Postgres)", "Tables, rows, physical size and how each is built; publish timings per refresh")
sz = pgdf("SELECT c.relname AS table_name, CASE WHEN c.relkind='v' THEN 'view' ELSE 'table' END AS kind, "
          "round(pg_total_relation_size(c.oid)/1e6, 3) AS size_mb FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
          "WHERE n.nspname='sdi_bi' AND c.relkind IN ('r','v') ORDER BY 3 DESC")
sz["rows"] = [int(pgdf(f"SELECT count(*) n FROM sdi_bi.{t}").n[0]) for t in sz.table_name]
sz["built_from"] = ["DuckDB gold " + ("star table (copied as-is)" if t in STAR_TABLES else "BI SQL (sdi/bi/publish.py)")
                    if t in STAR_TABLES or t in BI_TABLES else "view over ops.*" for t in sz.table_name]
r = write_df(ws, sz[["table_name", "kind", "rows", "size_mb", "built_from"]], 4, name="serving")
n = len(sz)
label(ws, f"G4", "Total rows"); fcell(ws, "H4", f"=SUM(C5:C{4 + n})", bold=True)
label(ws, f"G5", "Total MB"); fcell(ws, "H5", f"=SUM(D5:D{4 + n})", "#,##0.0", bold=True)
ws.column_dimensions["G"].width = 14
r = section(ws, r, "Publish log (every table, every refresh)")
r = write_df(ws, publog, r, name="publog")
r = section(ws, r, "BI table SQL (DuckDB, gold → serving)")
write_df(ws, pd.DataFrame([dict(table=t, sql=" ".join(s.split())) for t, s in BI_TABLES.items()]), r, name="bisql",
         widths={"sql": 140}, wrap=("sql",))

# ============================================================================================ 07 Filters & interactivity
ws = sheet("07_Filters_Interactivity", "Native filters, cross-filtering and drill", "Filter bar per dashboard, scoped to charts whose dataset has the column")
fr = []
for d in ss.DASHBOARDS:
    did = api.find("dashboard", "slug", d["slug"])
    meta = json.loads(api.call("GET", f"dashboard/{did}")["result"]["json_metadata"])
    for f in meta.get("native_filter_configuration", []):
        col = (f.get("targets") or [{}])[0].get("column", {}).get("name", "time range")
        fr.append(dict(dashboard=d["title"].split(" · ")[1], filter=f["name"], type=f["filterType"], column=col,
                       charts_in_scope=len(f.get("chartsInScope", [])), charts_excluded=len(f["scope"]["excluded"]),
                       default=json.dumps(f["defaultDataMask"].get("filterState", {}).get("value")) if
                       f["defaultDataMask"].get("filterState", {}).get("value") else ""))
r = write_df(ws, pd.DataFrame(fr), 4, name="filters")
for t in ["Cross-filtering: click a bar / slice / cell to filter every other chart on the dashboard (DASHBOARD_CROSS_FILTERS).",
          "Drill to detail: right-click any data point → see the underlying rows (DRILL_TO_DETAIL).",
          "Drill by: right-click → re-slice the same metric by another dimension (DRILL_BY).",
          "Filter scoping: a filter only applies to charts whose dataset contains its column; the others are listed in "
          "charts_excluded, so no chart breaks when a filter is set.",
          "Default: BI-2 starts with Date quality = Normal (year-start bulk postings excluded)."]:
    r = note(ws, r, "• " + t, cols=8, italic=False)

# ============================================================================================ 08 Refresh evidence
ws = sheet("08_Refresh_Evidence", "Refresh evidence — the dashboards changed with the new data",
           "KPI values snapshotted after each refresh (inputs, blue) and the change between refreshes (formulas)")
r = 4
hdr = ["order", "dashboard", "kpi", "unit"] + tags + ["Δ R1−BASE", "Δ R2−R1", "Δ R2−BASE", "% change BASE→R2"]
for j, h in enumerate(hdr, start=1):
    label(ws, f"{get_column_letter(j)}{r}", h, fill=LIGHT)
for i, row in enumerate(kpi_piv.itertuples(index=False), start=1):
    rr = r + i
    vals = [row.kpi_order, row.dashboard, row.kpi_name, row.unit] + [getattr(row, t) for t in tags]
    for j, v in enumerate(vals, start=1):
        c = ws.cell(row=rr, column=j, value=float(v) if isinstance(v, float) else v)
        c.font = Font(name=FONT, size=9, color="0000FF" if j > 4 else "000000")
        c.border = BORDER
        if j > 4:
            c.number_format = "#,##0.00"
    if len(tags) == 3:
        fcell(ws, f"H{rr}", f"=F{rr}-E{rr}", "#,##0.00;[Red]-#,##0.00")
        fcell(ws, f"I{rr}", f"=G{rr}-F{rr}", "#,##0.00;[Red]-#,##0.00")
        fcell(ws, f"J{rr}", f"=G{rr}-E{rr}", "#,##0.00;[Red]-#,##0.00")
        fcell(ws, f"K{rr}", f"=IF(E{rr}=0,0,(G{rr}-E{rr})/ABS(E{rr}))", "0.00%;[Red]-0.00%")
nk = len(kpi_piv)
ws.column_dimensions["B"].width = 16; ws.column_dimensions["C"].width = 46
for L in "EFGHIJK":
    ws.column_dimensions[L].width = 15
r += nk + 2
label(ws, f"B{r}", "KPIs that changed BASE→R2"); fcell(ws, f"C{r}", f'=COUNTIF(J{5}:J{4 + nk},"<>0")', bold=True)
r += 2
r = section(ws, r, "Refresh log (ops.bi_refresh_log) with formulas for what each refresh added")
rl0 = r
r = write_df(ws, refresh_log, r, name="refreshlog")
label(ws, f"L{rl0}", "sales rows added"); label(ws, f"M{rl0}", "hospital rows added"); label(ws, f"N{rl0}", "days of data added")
for i in range(1, len(refresh_log) + 1):
    rr = rl0 + i
    if i == 1:
        for L in "LMN":
            ws[f"{L}{rr}"] = "-"
    else:
        fcell(ws, f"L{rr}", f"=E{rr}-E{rr - 1}")
        fcell(ws, f"M{rr}", f"=F{rr}-F{rr - 1}")
        fcell(ws, f"N{rr}", f"=C{rr}-C{rr - 1}", "0")
if len(tags) == 3:
    ch = BarChart(); ch.type = "col"; ch.title = "Supply gap & unfulfilled demand after each refresh"; ch.style = 10
    row_gap = 5 + list(kpi_piv.kpi_name).index("Supply gap (packs)")
    data = Reference(ws, min_col=5, max_col=7, min_row=row_gap, max_row=row_gap)
    ch.add_data(data, from_rows=True, titles_from_data=False)
    ch.set_categories(Reference(ws, min_col=5, max_col=7, min_row=4, max_row=4))
    ch.height, ch.width = 7, 14; ch.legend = None; chart_style(ch)
    ws.add_chart(ch, f"B{r + 1}")
    ch2 = BarChart(); ch2.type = "col"; ch2.title = "Sales transactions in the warehouse after each refresh"; ch2.style = 10
    row_tx = 5 + list(kpi_piv.kpi_name).index("Sales transactions in mart")
    ch2.add_data(Reference(ws, min_col=5, max_col=7, min_row=row_tx, max_row=row_tx), from_rows=True, titles_from_data=False)
    ch2.set_categories(Reference(ws, min_col=5, max_col=7, min_row=4, max_row=4))
    ch2.height, ch2.width = 7, 14; ch2.legend = None; chart_style(ch2)
    ws.add_chart(ch2, f"H{r + 1}")

# ============================================================================================ 09 Data by refresh
ws = sheet("09_Data_By_Refresh", "Chart data as it looked after each refresh",
           "Re-computed from the serving layer by refresh tag (BASE ⊂ R1 ⊂ R2): what each dashboard showed at each point in time")
r = 4
cum = {"BASE": "('BASE')", "R1": "('BASE','R1')", "R2": "('BASE','R1','R2')"}
r = section(ws, r, "BI-1 · latest 6 months: forecast vs actual consumption (as shown after each refresh)")
parts = []
for t in tags:
    x = pgdf(f"""SELECT year_month, sum(forecast_packs) forecast, sum(consumed_packs) consumed,
                 round((100.0*sum(forecast_error_packs)/sum(consumed_packs))::numeric,3) bias_pct
                 FROM sdi_bi.bi_hospital_demand WHERE refresh_tag IN {cum[t]} GROUP BY 1 ORDER BY 1 DESC LIMIT 6""")
    x.insert(0, "after_refresh", t)
    parts.append(x)
r = write_df(ws, pd.concat(parts), r, name="hosp_by_refresh")
r = section(ws, r, "BI-2 · latest 4 months: net revenue by channel (normal postings)")
parts = []
for t in tags:
    x = pgdf(f"""SELECT year_month, channel_group, round(sum(net_usd)::numeric,2) net_usd FROM sdi_bi.bi_sales_monthly
                 WHERE refresh_tag IN {cum[t]} AND date_quality_flag='Normal' AND year_month >= (SELECT to_char(max(month_date)
                 - interval '3 months','YYYY-MM') FROM sdi_bi.bi_sales_monthly WHERE refresh_tag IN {cum[t]})
                 GROUP BY 1,2 ORDER BY 1 DESC, 2""")
    x.insert(0, "after_refresh", t)
    parts.append(x)
r = write_df(ws, pd.concat(parts), r, name="chan_by_refresh")
r = section(ws, r, "BI-3 · slowest 6 medicines (packs / active month) after each refresh — new launches enter in R2")
parts = []
for t in tags:
    x = pgdf(f"""WITH s AS (SELECT generic_name, cold_chain_flag, sum(packs) packs, count(DISTINCT year_month) m
                 FROM sdi_bi.bi_sales_monthly WHERE refresh_tag IN {cum[t]} AND date_quality_flag='Normal' GROUP BY 1,2)
                 SELECT generic_name, cold_chain_flag, round(packs::numeric/m, 1) packs_per_month FROM s
                 ORDER BY 3 LIMIT 6""")
    x.insert(0, "after_refresh", t)
    parts.append(x)
r = write_df(ws, pd.concat(parts), r, name="vel_by_refresh")
r = section(ws, r, "BI-4 · unified demand by channel, latest month present after each refresh")
parts = []
for t in tags:
    x = pgdf(f"""WITH m AS (SELECT max(year_month) ym FROM sdi_bi.bi_sales_monthly WHERE refresh_tag IN {cum[t]})
                 SELECT (SELECT ym FROM m) AS month, channel_group, sum(demand_packs) demand_packs
                 FROM sdi_bi.bi_unified_demand WHERE year_month = (SELECT ym FROM m) GROUP BY 1,2 ORDER BY 3 DESC""")
    x.insert(0, "after_refresh", t)
    parts.append(x)
write_df(ws, pd.concat(parts), r, name="uni_by_refresh")

# ============================================================================================ 10 Live chart data
ws = sheet("10_Chart_Data_Live", "Data behind every dashboard chart (fetched through Superset, after the latest refresh)",
           "GET /api/v1/chart/{id}/data — exactly what each visual renders (first 40 rows per chart)")
r = 4
for d in ss.DASHBOARDS:
    did = api.find("dashboard", "slug", d["slug"])
    for c in api.call("GET", f"dashboard/{did}/charts")["result"]:
        if c["slice_name"] == "SDI · Data refresh status" and d is not ss.DASHBOARDS[0]:
            continue
        res = api.call("GET", f"chart/{c['id']}/data/")["result"][0]
        df = pd.DataFrame(res.get("data", [])).head(40)
        for col in df.columns:
            if df[col].dtype.kind in "iuf" and df[col].abs().max() > 1e11:   # epoch ms timestamps
                df[col] = pd.to_datetime(df[col], unit="ms")
        r = section(ws, r, f"{d['title'].split(' · ')[1]} · {c['slice_name']}  (chart id {c['id']}, {res.get('rowcount')} rows)")
        if len(df):
            df.columns = [str(x)[:60] for x in df.columns]
            r = write_df(ws, df, r, max_w=28)
        else:
            r += 1

# ============================================================================================ 11 Mini dashboard (interactive)
hosp = pgdf("""SELECT hospital_name, disease_category, year_month, sum(forecast_packs) forecast, sum(consumed_packs) consumed,
               sum(requested_packs) requested, sum(received_packs) received, sum(unfulfilled_packs) unfulfilled,
               sum(emergency_packs) emergency, sum(abs_error_packs) abs_error FROM sdi_bi.bi_hospital_demand GROUP BY 1,2,3 ORDER BY 1,2,3""")
sales = pgdf("""SELECT year_month, channel_group, census_region, round(sum(net_usd)::numeric,2)::float net_usd,
                round(sum(margin_usd)::numeric,2)::float margin_usd, sum(packs) packs, sum(txn_count) txns
                FROM sdi_bi.bi_sales_monthly WHERE date_quality_flag='Normal' GROUP BY 1,2,3 ORDER BY 1,2,3""")
months = sorted(hosp.year_month.unique())
dws = wb.create_sheet("20_Data_Hospital")
dws["A1"] = "Data for 11_Mini_Dashboard (hospital x disease x month, from sdi_bi.bi_hospital_demand)"
write_df(dws, hosp, 3, name="dhosp")
sws = wb.create_sheet("21_Data_Sales")
sws["A1"] = "Data for 11_Mini_Dashboard (month x channel x region, normal postings, from sdi_bi.bi_sales_monthly)"
write_df(sws, sales, 3, name="dsales")
lws = wb.create_sheet("22_Lists")
for j, (h, vals) in enumerate([("Hospital", ["(All)"] + sorted(hosp.hospital_name.unique())),
                               ("Disease", ["(All)"] + sorted(hosp.disease_category.unique())),
                               ("Channel", ["(All)"] + sorted(sales.channel_group.unique())),
                               ("Region", ["(All)"] + sorted(sales.census_region.unique()))], start=1):
    lws.cell(row=1, column=j, value=h).font = Font(name=FONT, bold=True)
    for i, v in enumerate(vals, start=2):
        lws.cell(row=i, column=j, value=v)
HN, SN = len(hosp) + 3, len(sales) + 3
ws = sheet("11_Mini_Dashboard", "Interactive mini-dashboard (pick values in the yellow cells)",
           "Excel twin of BI-1 and BI-2: dropdowns drive SUMIFS formulas and the two charts below")
ws.column_dimensions["A"].width = 30
for L in "BCDEFGHI":
    ws.column_dimensions[L].width = 15
label(ws, "A4", "Hospital"); label(ws, "A5", "Disease category"); label(ws, "D4", "Channel"); label(ws, "D5", "Region")
for ref, val in (("B4", "(All)"), ("B5", "(All)"), ("E4", "(All)"), ("E5", "(All)")):
    ws[ref] = val
    ws[ref].fill = PatternFill("solid", fgColor=YELLOW)
    ws[ref].font = Font(name=FONT, size=10, bold=True)
    ws[ref].border = BORDER
for ref, colL, n in (("B4", "A", 13), ("B5", "B", 8), ("E4", "C", 4), ("E5", "D", 5)):
    dv = DataValidation(type="list", formula1=f"='22_Lists'!${colL}$2:${colL}${n + 1}", allow_blank=False)
    ws.add_data_validation(dv)
    dv.add(ws[ref])
ws["G4"] = '=IF(B4="(All)","*",B4)'; ws["G5"] = '=IF(B5="(All)","*",B5)'
ws["H4"] = '=IF(E4="(All)","*",E4)'; ws["H5"] = '=IF(E5="(All)","*",E5)'
for c in ("G4", "G5", "H4", "H5"):
    ws[c].font = Font(name=FONT, size=8, color="808080")
H = lambda col: f"'20_Data_Hospital'!${col}$4:${col}${HN}"
S = lambda col: f"'21_Data_Sales'!${col}$4:${col}${SN}"
hc = f"{H('A')},$G$4,{H('B')},$G$5"
sc = f"{S('B')},$H$4,{S('C')},$H$5"
r = 7
r = section(ws, r, "BI-1 · Hospital KPIs for the selection")
kp = [("Forecast (packs)", f"=SUMIFS({H('D')},{hc})", "#,##0"), ("Consumed (packs)", f"=SUMIFS({H('E')},{hc})", "#,##0"),
      ("Forecast bias", "=IF(B9=0,0,(B8-B9)/B9)", "0.00%"), ("Forecast error (WAPE)", f"=IF(B9=0,0,SUMIFS({H('J')},{hc})/B9)", "0.00%"),
      ("Requested (packs)", f"=SUMIFS({H('F')},{hc})", "#,##0"), ("Received (packs)", f"=SUMIFS({H('G')},{hc})", "#,##0"),
      ("Fill rate", "=IF(B12=0,0,B13/B12)", "0.0%"), ("Supply gap (packs)", "=B12-B13", "#,##0"),
      ("Unfulfilled (packs)", f"=SUMIFS({H('H')},{hc})", "#,##0"), ("Emergency (packs)", f"=SUMIFS({H('I')},{hc})", "#,##0")]
for i, (k, f, fmt) in enumerate(kp):
    label(ws, f"A{r + i}", k)
    fcell(ws, f"B{r + i}", f, fmt, bold=True)
r2 = 7
section(ws, r2, "BI-2 · Channel KPIs for the selection", col=4)
kp2 = [("Net revenue ($)", f"=SUMIFS({S('D')},{sc})", "$#,##0"), ("Gross margin ($)", f"=SUMIFS({S('E')},{sc})", "$#,##0"),
       ("Gross margin %", "=IF(E8=0,0,E9/E8)", "0.0%"), ("Packs", f"=SUMIFS({S('F')},{sc})", "#,##0"),
       ("Transactions", f"=SUMIFS({S('G')},{sc})", "#,##0"), ("Avg packs / transaction", "=IF(E12=0,0,E11/E12)", "0.00")]
for i, (k, f, fmt) in enumerate(kp2):
    label(ws, f"D{r2 + 1 + i}", k)
    fcell(ws, f"E{r2 + 1 + i}", f, fmt, bold=True)
r = 20
r = section(ws, r, "Monthly series for the selection (drives the charts)")
for j, h in enumerate(["Month", "Forecast", "Consumed", "Bias", "Net revenue", "Gross margin %"], start=1):
    label(ws, f"{get_column_letter(j)}{r}", h)
for i, m in enumerate(months, start=1):
    rr = r + i
    ws.cell(row=rr, column=1, value=m).font = Font(name=FONT, size=9)
    fcell(ws, f"B{rr}", f'=SUMIFS({H("D")},{hc},{H("C")},$A{rr})')
    fcell(ws, f"C{rr}", f'=SUMIFS({H("E")},{hc},{H("C")},$A{rr})')
    fcell(ws, f"D{rr}", f"=IF(C{rr}=0,0,(B{rr}-C{rr})/C{rr})", "0.0%")
    fcell(ws, f"E{rr}", f'=SUMIFS({S("D")},{sc},{S("A")},$A{rr})', "$#,##0")
    fcell(ws, f"F{rr}", f'=IF(E{rr}=0,0,SUMIFS({S("E")},{sc},{S("A")},$A{rr})/E{rr})', "0.0%")
last = r + len(months)
lc = LineChart(); lc.title = "Forecast vs consumed (selection)"; lc.style = 12; lc.height, lc.width = 8, 18
lc.add_data(Reference(ws, min_col=2, max_col=3, min_row=r, max_row=last), titles_from_data=True)
lc.set_categories(Reference(ws, min_col=1, min_row=r + 1, max_row=last)); chart_style(lc)
ws.add_chart(lc, "H20")
bc = BarChart(); bc.type = "col"; bc.title = "Net revenue by month (selection)"; bc.style = 10; bc.height, bc.width = 8, 18
bc.add_data(Reference(ws, min_col=5, min_row=r, max_row=last), titles_from_data=True)
bc.set_categories(Reference(ws, min_col=1, min_row=r + 1, max_row=last)); bc.legend = None; chart_style(bc)
ws.add_chart(bc, "H38")

# ============================================================================================ 12 Insights
ws = sheet("12_Insights", "Insights & recommendations (the story the dashboards tell)", "Each insight cites the KPI it comes from (sheet 08) and the visual that shows it")
K = {row.kpi_name: getattr(row, tags[-1]) for row in kpi_piv.itertuples()}
ins = pd.DataFrame([
    ("BI-1", "Hospitals systematically under-forecast", f"Forecast bias {K['Forecast bias %']:.2f}% (all 7 categories negative); WAPE {K['Forecast error (WAPE) %']:.2f}%",
     "Forecast bias by month, Forecast vs actual", "Add the measured bias as a correction factor to hospital forecasts; review monthly"),
    ("BI-1", "Demand is lost between request and receipt", f"Fill rate {K['Request-to-receipt fill rate %']:.1f}%; supply gap {K['Supply gap (packs)']:,.0f} packs (grows every refresh)",
     "Funnel, Supply gap by hospital", "Investigate approval cuts first (largest single loss), then delivery shortfalls"),
    ("BI-1", "A few hospitals carry most unfulfilled / emergency demand", f"Unfulfilled {K['Unfulfilled demand (packs)']:,.0f} packs; emergency {K['Emergency demand %']:.2f}% of consumption",
     "Hotspot table, Heatmap", "Priority allocation + safety stock for the top hotspot hospital/class pairs"),
    ("BI-2", "Retail dominates; E-commerce grows fastest", f"E-commerce {K['E-commerce share of revenue %']:.1f}% of revenue; hospital pharmacy {K['Hospital-pharmacy share of revenue %']:.1f}%",
     "Donut, Revenue by year & channel", "Plan e-commerce capacity growth; protect retail availability"),
    ("BI-2", "Margin is flat in every channel", f"Gross margin {K['Gross margin %']:.2f}% everywhere", "Margin % by channel & year",
     "Channel strategy is about volume and service, not price"),
    ("BI-2", "E-commerce fulfilment is failing", f"{K['E-commerce returned-to-sender %']:.1f}% of e-commerce orders returned to sender",
     "RTS rate by month", "Fix address validation / courier SLAs before pushing more online volume"),
    ("BI-3", "Cold-chain medicines: high value, slow moving", f"{K['Cold-chain share of packs %']:.2f}% of packs but {K['Cold-chain share of revenue %']:.1f}% of revenue",
     "Slow-movers bar, Bubble chart", "Small, frequent cold-chain batches; tighter expiry monitoring (Phase 2/3)"),
    ("BI-3", "New launches ramp slowly", f"{int(K['Medicines on sale'])} medicines after R2 (2 launches)", "Slow-movers bar, League table",
     "Launch stock sized to early velocity, not to mature-product averages"),
    ("BI-4", "Sales data alone hides most demand", f"Hospital wards = {K['Hospital-ward share of unified demand %']:.1f}% of unified demand packs",
     "Unified demand by channel, Sunburst", "Plan production on the unified demand signal (BI-4), not on sales"),
    ("BI-4", "The pipeline keeps the BI current", f"{int(K['Months of history'])} months of history, {int(K['Sales transactions in mart']):,} transactions after R2",
     "Refresh banner, KPI-by-refresh pivot, Refresh log", "Dashboards refresh automatically after every load (sdi_06)"),
], columns=["Dashboard", "Insight", "Evidence (latest refresh)", "Visual", "Recommended action"])
write_df(ws, ins, 4, name="insights", widths={"Insight": 40, "Evidence (latest refresh)": 60, "Visual": 34,
                                               "Recommended action": 60}, wrap=tuple(ins.columns))

# ============================================================================================ 13 BI performance
ws = sheet("13_BI_Performance", "BI query performance (every Superset chart fetch, timed by sdi_06)",
           "Counter incremented every time a BI component fetches data; logged to ops.query_log (caller = bi_dashboard)")
perf = bi_q.groupby(["dashboard", "widget_name"]).agg(fetches=("exec_ms", "size"), avg_ms=("exec_ms", "mean"),
                                                      max_ms=("exec_ms", "max"), rows=("rows_returned", "max")).reset_index()
perf["avg_ms"] = perf.avg_ms.astype(float).round(1); perf["max_ms"] = perf.max_ms.astype(float).round(1)
perf = perf.sort_values("avg_ms", ascending=False)
pstart = 4
r = write_df(ws, perf, pstart, name="perf", widths={"dashboard": 46, "widget_name": 52})
np_ = len(perf)
label(ws, "H4", "Chart fetches (all refreshes)"); fcell(ws, "I4", f"=SUM(C5:C{4 + np_})", bold=True)
label(ws, "H5", "Average fetch (ms)"); fcell(ws, "I5", f"=AVERAGE(D5:D{4 + np_})", "#,##0.0", bold=True)
label(ws, "H6", "Slowest chart (ms)"); fcell(ws, "I6", f"=MAX(E5:E{4 + np_})", "#,##0.0", bold=True)
label(ws, "H7", "Failed fetches"); ws["I7"] = int((bi_q.status != "SUCCESS").sum()); ws["I7"].font = Font(name=FONT, size=9, color="0000FF")
ws.column_dimensions["H"].width = 26
r = section(ws, r, "Fetch time by refresh")
byr = bi_q.groupby("refresh_tag").agg(fetches=("exec_ms", "size"), avg_ms=("exec_ms", "mean"), p95_ms=("exec_ms", lambda s: s.quantile(.95)),
                                      failed=("status", lambda s: int((s != "SUCCESS").sum()))).reset_index()
for c in ("avg_ms", "p95_ms"):
    byr[c] = byr[c].astype(float).round(1)
write_df(ws, byr, r, name="perfref")

# ============================================================================================ 14 Reconciliation tests
ws = sheet("14_Reconciliation_Tests", "BI reconciliation tests (dashboard numbers = warehouse numbers)",
           "Each test computes the same figure on the BI serving layer, the DuckDB gold mart and Postgres staging; PASS by formula")
duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
dk = lambda s: duck.execute(s).fetchone()[0]
pgv = lambda s: pd.read_sql(s, pg).iloc[0, 0]
tests = [
    ("T01", "Sales transactions", pgv("SELECT count(*) FROM sdi_bi.fact_sales"), dk("SELECT count(*) FROM fact_sales"),
     pgv("SELECT count(*) FROM staging.stg_pharmacy_sales_data")),
    ("T02", "Net sales USD (all postings)", pgv("SELECT round(sum(net_usd)::numeric,2) FROM sdi_bi.bi_sales_monthly"),
     dk("SELECT round(sum(net_sales_usd),2) FROM fact_sales"), pgv("SELECT round(sum(net_sales_amount_usd)::numeric,2) FROM staging.stg_pharmacy_sales_data")),
    ("T03", "Hospital consumed packs", pgv("SELECT sum(consumed_packs) FROM sdi_bi.bi_hospital_demand"),
     dk("SELECT sum(consumed_qty_packs) FROM fact_hospital_demand"), pgv("SELECT sum(consumed_quantity_packs) FROM staging.stg_hospital_demand_data")),
    ("T04", "Hospital requested packs", pgv("SELECT sum(requested_packs) FROM sdi_bi.bi_hospital_demand"),
     dk("SELECT sum(requested_qty_packs) FROM fact_hospital_demand"), pgv("SELECT sum(requested_quantity_packs) FROM staging.stg_hospital_demand_data")),
    ("T05", "Funnel 'Requested' stage = requested packs", pgv("SELECT sum(packs) FROM sdi_bi.bi_supply_funnel WHERE stage_order=1"),
     dk("SELECT sum(requested_qty_packs) FROM fact_hospital_demand"), pgv("SELECT sum(requested_quantity_packs) FROM staging.stg_hospital_demand_data")),
    ("T06", "Unified demand = normal sales packs + ward consumption", pgv("SELECT sum(demand_packs) FROM sdi_bi.bi_unified_demand"),
     dk("SELECT (SELECT sum(qty_packs) FROM agg_sales_monthly WHERE date_quality_flag='Normal') + (SELECT sum(consumed_qty_packs) FROM fact_hospital_demand)"),
     None),
    ("T07", "Medicines on sale (velocity table)", pgv("SELECT count(*) FROM sdi_bi.bi_medicine_velocity"),
     dk("SELECT count(DISTINCT m.medicine_id) FROM fact_sales f JOIN dim_medicine m USING (medicine_key)"), None),
]
tdf = pd.DataFrame([dict(test=a, check=b, bi_serving_layer=float(c), gold_mart_duckdb=float(d),
                         staging=float(e) if e is not None else None) for a, b, c, d, e in tests])
ts0 = 4
write_df(ws, tdf, ts0, widths={"check": 48})
label(ws, f"F{ts0}", "diff BI vs mart"); label(ws, f"G{ts0}", "diff mart vs staging"); label(ws, f"H{ts0}", "result")
for i in range(1, len(tdf) + 1):
    rr = ts0 + i
    fcell(ws, f"F{rr}", f"=C{rr}-D{rr}", "#,##0.00")
    fcell(ws, f"G{rr}", f'=IF(E{rr}="","n/a",D{rr}-E{rr})', "#,##0.00")
    fcell(ws, f"H{rr}", f'=IF(AND(ABS(F{rr})<0.01,OR(G{rr}="n/a",ABS(N(G{rr}))<0.01)),"PASS","FAIL")', "@", bold=True)
label(ws, f"A{ts0 + len(tdf) + 2}", "Tests passed")
fcell(ws, f"B{ts0 + len(tdf) + 2}", f'=COUNTIF(H{ts0 + 1}:H{ts0 + len(tdf)},"PASS")&" / {len(tdf)}"', "@", bold=True)

# ============================================================================================ 15-17 Screenshots
for tag in tags:
    ws = sheet(f"15_Screens_{tag}" if tag == "BASE" else f"{15 + tags.index(tag)}_Screens_{tag}",
               f"Superset dashboards after refresh {tag}", "Headless-Chrome screenshots taken by scripts/screenshot_bi.py right after the refresh")
    r = 4
    for slug in ["sdi-bi-1-hospital", "sdi-bi-2-channel", "sdi-bi-3-velocity", "sdi-bi-4-unified"]:
        p = SHOTS / tag / f"{slug}.png"
        ws.cell(row=r, column=1, value=slug).font = Font(name=FONT, bold=True, color=BLUE)
        h = image(ws, p, f"A{r + 1}", width=760)
        r += int(h / 20) + 4

# ============================================================================================ 18 User guide
ws = sheet("18_User_Guide", "User guide", "How to open, read and refresh the BI dashboards")
guide = pd.DataFrame([
    ("Open", "http://localhost:8088 → login admin/admin → Dashboards → 'SDI · BI-1 … BI-4'"),
    ("Filter", "Left filter bar: time range, region, hospital, channel, disease, therapeutic class, cold chain, velocity class → Apply"),
    ("Cross-filter", "Click any bar, slice or table cell to filter the whole dashboard; click again to clear"),
    ("Drill", "Right-click a data point → 'Drill to detail' (underlying rows) or 'Drill by' (another dimension)"),
    ("Refresh status", "Top banner on every dashboard: refresh tag, data-as-of date, publish time, row counts"),
    ("Auto-refresh", "Dashboards re-query every 30 s; results are never cached, so a pipeline refresh appears within 30 s"),
    ("Trigger a refresh", "Airflow → sdi_99_refresh_cycle → Trigger with refresh_tag BASE / R1 / R2 (or drop files in data/landing)"),
    ("Refresh only the BI", "Airflow → sdi_06_bi_refresh → Trigger (publishes the current mart and re-syncs Superset)"),
    ("Re-create dashboards", "python -m sdi.bi.superset provision (dashboards as code; idempotent)"),
    ("SQL", "Superset SQL Lab → database 'SDI BI (sdi_bi serving layer)' → schema sdi_bi (read-only bi_reader)"),
], columns=["Task", "How"])
write_df(ws, guide, 4, name="guide", widths={"How": 110})

# ============================================================================================ 19 Assumptions
ws = sheet("19_Assumptions", "BI assumptions and design decisions", "Explicit assumptions behind the BI layer")
ass = pd.DataFrame([
    ("B01", "Tool", "Apache Superset (open source, runs in Docker on macOS/Windows/Linux); Power BI Desktop does not run on macOS."),
    ("B02", "Serving", "BI reads a Postgres copy of the gold mart (sdi_bi), never the DuckDB file: DuckDB allows one writer and blocks other processes while Airflow writes."),
    ("B03", "Freshness", "Query results are not cached and dashboards auto-refresh every 30 s; BI tables are pre-aggregated so live queries stay < 100 ms."),
    ("B04", "Consistency", "The serving schema is built aside and swapped in one transaction, so a dashboard never shows a half-loaded refresh."),
    ("B05", "Security", "Superset connects with a read-only role (bi_reader); admin/admin is for the local demo only."),
    ("B06", "Date quality", "Year-start bulk postings (22% of sales) are excluded from revenue trends by default (filter Date quality = Normal) but remain available."),
    ("B07", "Velocity", "Velocity = packs sold per ACTIVE month, so new launches are compared fairly with established medicines."),
    ("B08", "Channels", "Retail = chain + independent pharmacies; Hospital pharmacy = physical hospital pharmacies; E-commerce; Hospital ward = hospital ERP consumption (unified demand only)."),
    ("B09", "KPIs", "Ratios are computed from sums (e.g. bias = Σ(forecast−consumed)/Σconsumed), never averaged, so they roll up correctly under any filter."),
    ("B10", "Refresh evidence", "KPIs are snapshotted after every refresh; 'data as of' = latest sale date in the warehouse."),
    ("B11", "Dashboards as code", "All Superset objects are created by sdi/bi/superset.py from version-controlled specs, so the BI layer is reproducible on any machine."),
], columns=["ID", "Area", "Assumption / decision"])
write_df(ws, ass, 4, name="assume", widths={"Assumption / decision": 140}, wrap=("Assumption / decision",))

# ============================================================================================ TOC + order
ws = wb["00_Cover"]
label(ws, f"A{toc_row}", "Sheet"); label(ws, f"B{toc_row}", "Contents")
for i, (name, title, purpose) in enumerate(TOC, start=1):
    c = ws.cell(row=toc_row + i, column=1, value=name)
    c.hyperlink = f"#'{name}'!A1"
    c.font = Font(name=FONT, size=9, color="0563C1", underline="single")
    d = ws.cell(row=toc_row + i, column=2, value=f"{title} — {purpose}")
    d.font = Font(name=FONT, size=9)
    d.alignment = Alignment(wrap_text=True)
ws.column_dimensions["B"].width = 120
for w_ in wb.worksheets:
    w_.sheet_properties.tabColor = NAVY if w_.title.startswith("0") else BLUE if w_.title[:2] in ("10", "11", "12", "13", "14") \
        else "548235" if w_.title.startswith(("15", "16", "17")) else "C55A11"
wb._sheets.sort(key=lambda s: s.title)
wb.active = 0
wb.save(OUT)
print("saved", OUT, "sheets:", len(wb.sheetnames))
