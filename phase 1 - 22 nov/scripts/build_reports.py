"""Builds docs/Phase.1.Report.pdf (max 5 pages) and docs/Oversampling.Report.pdf (1-2 pages) from the live pipeline."""
import sys
import json
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
                                TableStyle, ListFlowable, ListItem)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN, DUCKDB_PATH
from sdi.generator.oversample import DIRTY_RATE, YOY_GROWTH, SCENARIOS

FIG = ROOT / "docs" / "figures"
pg = psycopg2.connect(PG_DSN)
duck = duckdb.connect(str(DUCKDB_PATH), read_only=True)
q = lambda s: pd.read_sql(s, pg)

ss = getSampleStyleSheet()
NAVY = colors.HexColor("#1F3864")
BODY = ParagraphStyle("body", parent=ss["Normal"], fontName="Helvetica", fontSize=11, leading=13.6, alignment=TA_JUSTIFY,
                      spaceAfter=4)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8.5, leading=10.5, textColor=colors.HexColor("#404040"))
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=15, textColor=NAVY, spaceAfter=4, spaceBefore=2)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=12.5, textColor=NAVY, spaceAfter=3, spaceBefore=6)
TITLE = ParagraphStyle("title", parent=H1, fontSize=17, leading=21, spaceAfter=2)
BUL = ParagraphStyle("bul", parent=BODY, alignment=0, spaceAfter=1.5)


def P(t, st=BODY):
    return Paragraph(t, st)


def bullets(items, st=BUL):
    return ListFlowable([ListItem(P(i, st), leftIndent=10, value="•") for i in items], bulletType="bullet",
                        start="•", leftIndent=12, bulletFontSize=9)


def is_screenshot(name):
    """Preview renders are saved by matplotlib (PNG 'Software' tag); real Grafana screenshots are not."""
    from PIL import Image as PILImage
    try:
        return "matplotlib" not in str(PILImage.open(FIG / name).info.get("Software", "")).lower()
    except Exception:
        return False


def fig(name, width_cm, caption=None, max_h_cm=None):
    from reportlab.lib.utils import ImageReader
    p = FIG / name
    iw, ih = ImageReader(str(p)).getSize()
    w = width_cm * cm
    h = w * ih / iw
    if max_h_cm and h > max_h_cm * cm:      # tall screenshots: scale down to keep the report within 5 pages
        h = max_h_cm * cm
        w = h * iw / ih
    out = [Image(str(p), width=w, height=h)]
    if caption:
        out.append(P(caption, SMALL))
    return out


def tbl(rows, widths, font=8.6):
    cell = ParagraphStyle("cell", parent=BODY, fontSize=font, leading=font + 1.8, alignment=0, spaceAfter=0)
    head = ParagraphStyle("head", parent=cell, fontName="Helvetica-Bold", textColor=colors.white)
    rows = [[Paragraph(str(v), head if i == 0 else cell) for v in r] for i, r in enumerate(rows)]
    t = Table(rows, colWidths=[w * cm for w in widths], repeatRows=1)
    t.setStyle(TableStyle([("FONT", (0, 0), (-1, -1), "Helvetica", font), ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", font),
                           ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2F5597")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                           ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BFBFBF")), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                           ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F2F2")]),
                           ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                           ("TOPPADDING", (0, 0), (-1, -1), 1.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5)]))
    return t


FOOTER = ["Phase.1.Report — SDI Data Mart · Data Analytics & Warehousing, Fall 2026"]


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#7F7F7F"))
    canvas.drawString(1.6 * cm, 1.0 * cm, FOOTER[0])
    canvas.drawRightString(A4[0] - 1.6 * cm, 1.0 * cm, f"Page {doc.page}")
    canvas.restoreState()


# ------------------------------------------------------------------------------------------- group members
GROUP_CSV = ROOT / "docs" / "group_members.csv"


def group_members():
    if not GROUP_CSV.exists():
        return []
    g = pd.read_csv(GROUP_CSV, dtype=str).fillna("")
    return [r for r in g.to_dict("records") if r.get("name", "").strip()]


_gm = group_members()
GROUP_LINE = ("Group: " + "; ".join(f"{m['name']} ({m['student_id']})" if m.get("student_id") else m["name"] for m in _gm)
              if _gm else "Group members: see README.md / SDI.Excel sheet 00_Cover.")

# ------------------------------------------------------------------------------------------- live numbers
batch = q("SELECT refresh_tag, count(*) n, sum(rows_read) rd, sum(rows_loaded) ld, sum(rows_rejected) rj, sum(rows_duplicate) dp, "
          "sum(duration_s) s, avg((status='SUCCESS')::int) ok FROM ops.batch_load_log GROUP BY 1 ORDER BY 1").set_index("refresh_tag")
sales_sheet = q("SELECT duration_s, read_s FROM ops.batch_load_log WHERE refresh_tag='BASE' AND source_name='05_pharmacy_sales_data'").iloc[0]
stream = q("SELECT sum(events_sent) sent, sum(events_ingested) ing, sum(events_malformed) bad, sum(events_duplicate) dup, "
           "avg(throughput_eps) eps, avg(avg_latency_ms) lat, max(p95_latency_ms) p95 FROM ops.stream_session").iloc[0]
stor = q("SELECT table_name, row_count, size_mb FROM ops.storage_snapshot WHERE snapshot_ts=(SELECT max(snapshot_ts) FROM ops.storage_snapshot)").set_index("table_name")
fp = q("SELECT * FROM ops.storage_footprint ORDER BY snapshot_ts DESC LIMIT 1").iloc[0]
qb = q("SELECT tier, avg(exec_ms) ms FROM ops.query_log WHERE caller='benchmark' AND status='SUCCESS' GROUP BY 1").set_index("tier").ms
qerr = q("SELECT count(*) n, count(*) FILTER (WHERE status='FAILED') f FROM ops.query_log").iloc[0]
fs = duck.execute("SELECT count(*), sum(net_sales_usd) FROM fact_sales").fetchone()
fh = duck.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]
stg_rows = int(stor[stor.index.str.startswith("stg_")].row_count.sum())
gen = q("SELECT refresh_tag, source_name, rows_generated, dirty_rows FROM ops.generator_log ORDER BY 1, 2")

# =========================================================================================== Phase.1.Report
story = []
story += [P("Phase 1 Report — Sales &amp; Demand Intelligence (SDI) Data Mart", TITLE),
          P("Automated DWH + BI pipeline on Apache Airflow · Pharmaceutical cold supply chain · Milestone 1 "
            "(implemented up to the query layer). " + GROUP_LINE, SMALL),
          Spacer(1, 4)]
story += [P("1. The problem we are solving", H2),
          P("<b>Planners cannot see true medicine demand, so hospitals are under-supplied and production is planned on "
            "incomplete signals.</b> Demand reaches the company through three disconnected systems at different grains: pharmacy "
            "POS (transactions), the e-commerce platform (orders) and hospital ERP exports (hospital × medicine × month). "
            "The provided 2023–2025 history shows the cost: hospitals requested 6.22 M packs but received 5.77 M "
            "(<b>7.3% supply gap</b>); forecasts under-shoot consumption in <b>all 7 disease categories</b> (bias −1.05%); "
            "<b>117,758 packs</b> were emergency requests (11% of hospital-months); Pain &amp; Inflammatory carries 40% of all "
            "stock-out days and one hospital 43% of unfulfilled demand. The SDI mart unifies the three demand streams on "
            "conformed dimensions (medicine, date, geography, channel) so these gaps become measurable — and answerable — "
            "through 13 business queries."),
          ]
story += [P("2. Architecture", H2)] + fig("architecture.png", 17.6,
           "Figure 1 — Five layers, all orchestrated by Airflow. Every layer writes its metrics to the Postgres <i>ops</i> "
           "schema, which is the only data source of the Grafana DE dashboards.")
story += [P("End-to-end run captured in this report", H2),
          tbl([["Run", "Batch sheets", "Rows loaded (batch)", "Rejected", "Dup. skipped", "Streamed events", "fact_sales after"],
               ["BASE (2023-25)", int(batch.loc["BASE", "n"]), f"{int(batch.loc['BASE','ld']):,}", int(batch.loc["BASE", "rj"]), int(batch.loc["BASE", "dp"]), "—", "500,000"],
               ["R1 (Jan-2026)", int(batch.loc["R1", "n"]), f"{int(batch.loc['R1','ld']):,}", int(batch.loc["R1", "rj"]), int(batch.loc["R1", "dp"]), "11,221 (+97 malformed)", "511,221"],
               ["R2 (Feb-2026)", int(batch.loc["R2", "n"]), f"{int(batch.loc['R2','ld']):,}", int(batch.loc["R2", "rj"]), int(batch.loc["R2", "dp"]), "11,257 (+98 malformed)", f"{fs[0]:,}"]],
              [2.6, 2.1, 2.9, 1.7, 2.0, 3.6, 2.6]),
          P("Staging and mart reconciled exactly after every run (row counts, net-sales $ and consumed packs), with zero "
            "orphan keys; the watermark only advances when reconciliation passes.", SMALL)]
story.append(PageBreak())

# --- page 2: Airflow design
story += [P("3. Design approach of the Airflow pipeline", H2)] + fig("dag_flow.png", 17.6, "Figure 2 — Seven DAGs; one per pipeline layer plus a one-click orchestrator.")
story += [bullets([
    "<b>One DAG per layer, one task per step.</b> Every task is a thin wrapper around one function of the <i>sdi</i> Python "
    "package, so the same code runs in Airflow, in the API and in tests. DAG files hold orchestration only (modular design).",
    "<b>Event-driven + scheduled.</b> The watchdog file-watcher registers each landing file through the ingestion API, "
    "which triggers <i>sdi_01</i> via the Airflow REST API; the DAG also polls the file registry every 2 minutes, so a missed "
    "event can never lose a file. Each DAG triggers the next (TriggerDagRunOperator); <i>sdi_99_refresh_cycle</i> chains all of "
    "them with <i>wait_for_completion</i> for the demo, branching between the BASE workbook and a generated refresh.",
    "<b>Dynamic task mapping</b> loads one mapped task per Excel file after the medicine master (referential checks need it). "
    "<b>Short-circuit tasks</b> skip a run when nothing is pending; a <b>PythonSensor</b> waits for the API before streaming.",
    "<b>Idempotent and restartable.</b> Staging inserts use ON CONFLICT DO NOTHING (re-delivered rows are counted as duplicates); "
    "fact loads anti-join on the degenerate transaction id; the transform extracts only rows above a watermark and commits "
    "the watermark only after reconciliation. Any task can simply be re-run.",
    "<b>Quality gates fail the DAG.</b> Reconciliation (staging = mart), the query benchmark's result-equivalence check (all "
    "tiers must return identical answers) and the date-coverage check (no missing month) raise errors. The equivalence gate "
    "fired for real in R2: an SCD Type-1 rename looked like a price version in the staging query; we fixed the query and re-ran.",
    "<b>Resilience and observability.</b> retries=2, retry_delay=1 min, 30-min timeouts, max_active_runs=1 (DuckDB has one "
    "writer, so mart-writing tasks are chained); success/failure callbacks write every task outcome to "
    "<i>ops.pipeline_task_log</i>.",
])]
story += [P("4. Layers in brief", H2),
          tbl([["Layer", "What we built"],
               ["Oversampling", "sdi_00 bootstraps one month for all 7 files (R1 = Jan-26, R2 = Feb-26), keeps batch -> shipment -> lot -> sale lineage, injects ~1% dirty rows and SCD changes."],
               ["Ingestion", "FastAPI ingestion API (file registration, streaming endpoint, query endpoint) + watchdog watcher; 9 validation rule types shared by batch and stream; rejects stored with reason codes."],
               ["Storage", "PostgreSQL 16 ODS: 7 append-only typed staging tables with lineage columns; GS1 ids kept as text; Excel local timestamps normalised to UTC."],
               ["Transform", "DuckDB fact constellation: fact_sales (transaction) + fact_hospital_demand (monthly snapshot); SCD2 medicine/hospital, SCD1 pharmacy, 2 junk dims, role-playing date (4 views), 5 aggregates."],
               ["Query", "13 business queries × 3 tiers (Postgres staging, star, optimised star), timed and logged; optimisation: aggregates, junk pre-filters, re-clustered fact (zone maps), ART indexes, partitioned Parquet."]],
              [2.6, 15.0], font=8.8)]
story.append(PageBreak())

# --- page 3: batch + stream dashboards
story += [P("5. Data-engineering dashboards (Grafana)", H2),
          P("Four Grafana dashboards (plus an overview) are provisioned from JSON and read only the <i>ops</i> schema; a "
            "<i>Refresh</i> selector compares BASE with R1/R2. " +
            ("Figures are screenshots of the Grafana dashboards after the BASE, R1 and R2 runs."
             if all(is_screenshot(f"dash_{d}.png") for d in ("batch", "stream", "storage", "query")) else
             "Figures are rendered from the same SQL as the Grafana panels."), SMALL)]
story += [P("5.1 Batch ingestion — <i>which file is slow, and is anything failing?</i>", H2)] + fig("dash_batch.png", 14.6, max_h_cm=8.2)
story += [P(f"<b>Story:</b> {int(batch.n.sum())} sheet loads, <b>100% success</b>. The BASE load moved "
            f"{int(batch.loc['BASE','ld']):,} rows in {batch.loc['BASE','s']:.0f} s; the 500 k-row sales sheet alone took "
            f"{sales_sheet.duration_s:.0f} s, of which {sales_sheet.read_s:.0f} s was parsing Excel — the bottleneck is reading, not "
            f"Postgres (COPY is cheap). Refresh files load in under 2 s. Every refresh shows its injected dirty rows being "
            f"rejected (unknown SKU, negative quantity, bad date…), and re-delivered master rows are skipped as duplicates, "
            f"proving idempotency.")]
story += [P("5.2 Streaming ingestion — <i>is the stream stable and fast?</i>", H2)] + fig("dash_stream.png", 14.6, max_h_cm=8.2)
story += [P(f"<b>Story:</b> {int(stream.ing):,} sales events committed through the API at ~{stream.eps:.0f} events/s against a "
            f"400/s target; end-to-end latency averages {stream.lat:.0f} ms (p95 {stream.p95:.0f} ms). {int(stream.bad)} malformed and "
            f"{int(stream.dup)} duplicate events were caught by the API's validation; 100% of events sent are accounted for "
            "(ingested + rejected). Dips in throughput coincide with micro-batches carrying rejects.")]
story.append(PageBreak())

# --- page 4: storage + query
story += [P("5.3 Storage &amp; staging — <i>how much, where, and is history complete?</i>", H2)] + fig("dash_storage.png", 14.6, max_h_cm=8.2)
st_s, f_s = stor.loc["stg_pharmacy_sales_data"], stor.loc["fact_sales"]
story += [P(f"<b>Story:</b> {stg_rows:,} staging rows across the 7 domains; sales and IoT dominate rows and MB. The same sales data "
            f"needs {st_s.size_mb:.0f} MB in the Postgres row store but {f_s.size_mb:.0f} MB in the DuckDB fact "
            f"(<b>{st_s.size_mb / f_s.size_mb:.0f}× smaller</b>, columnar compression). History is appended, never truncated, and "
            "every month from 2023-01 to 2026-02 is present. The coverage chart also exposes a data-quality issue: "
            "the January spikes are year-start bulk postings (22% of sales), which the transform flags.")]
story += [P("5.4 Query layer — <i>does the warehouse make questions fast?</i>", H2)] + fig("dash_query.png", 14.6, max_h_cm=8.2)
story += [P(f"<b>Story:</b> {int(qerr.n):,} timed executions. Answering the business questions on Postgres staging takes "
            f"~{qb['oltp_staging']:.0f} ms on average (up to ~0.5 s); the star schema needs ~{qb['star_schema']:.0f} ms and the "
            f"optimised tier ~{qb['star_optimized']:.1f} ms — <b>≈{qb['oltp_staging'] / qb['star_optimized']:.0f}× faster</b>, and stable "
            f"as data grows across refreshes. The error rate ({100 * qerr.f / qerr.n:.2f}%) comes from simulated ad-hoc analyst "
            "queries with deliberate mistakes, proving errors are captured.")]
story.append(PageBreak())

# --- page 5: model + assumptions
story += [P("6. Dimensional model (transformation layer)", H2)] + fig("star_schema.png", 15.5)
story += [P("Fact types: transaction (sales) and periodic snapshot (hospital demand); additive, semi-additive (stock) and "
            "non-additive measures (ratios, disease activity) are documented column by column in SDI.Excel (sheets 09–14), "
            "together with the bus matrix that shows how Phase 2/3 facts will reuse the conformed dimensions.", SMALL)]
story += [P("7. Key assumptions", H2), bullets([
    "The provided workbook is the system of record for the BASE load. Its timestamps are UTC+05:00 local time "
    "(verified: exactly +5 h on every row versus the CSV export) and are converted to UTC on load.",
    "Year-start bulk postings (1 Jan sales posted > 240 days after delivery) are kept but flagged; trend, velocity and "
    "unified-demand KPIs use normal sales only.",
    "Channels: Retail = chain + independent pharmacies; Hospital = hospital pharmacies; E-commerce; plus Hospital ward "
    "(ERP consumption) in the unified demand view. Margin is identical (23.4%) in every channel.",
    "Refresh sales arrive as an event stream; all other increments arrive as Excel batch files. Each refresh adds one "
    "month (~25 k rows, ~2.4% of history), so both refreshes move every dashboard while history stays dominant.",
    "Postgres footprint uses pg_database_size / pg_total_relation_size; DuckDB uses os.path.getsize and storage blocks.",
])]
story += [P("How to run: <i>docker compose up -d --build</i>, copy the provided files into <i>data/source/</i>, then trigger "
            "<i>sdi_99_refresh_cycle</i> with BASE, R1, R2 (Airflow :8080, Grafana :3000, API :8000/docs). Full details: README.md.", SMALL)]

doc = SimpleDocTemplate(str(ROOT / "docs" / "Phase.1.Report.pdf"), pagesize=A4, leftMargin=1.6 * cm, rightMargin=1.6 * cm,
                        topMargin=1.3 * cm, bottomMargin=1.6 * cm, title="Phase.1.Report - SDI Data Mart",
                        author="SDI group")
doc.build(story, onFirstPage=footer, onLaterPages=footer)

# =========================================================================================== Oversampling report
s2 = []
s2 += [P("Oversampling Report — Data Generation Methodology", TITLE),
       P("SDI Phase 1 · Data Oversampling Layer (DAG sdi_00_data_generator, code: sdi/generator/oversample.py)", SMALL), Spacer(1, 4)]
s2 += [P("Why we generate data, and how much", H2),
       P("The pipeline must show the data warehouse and dashboards changing as new data arrives, at least twice, without "
         "emptying the warehouse. We therefore generate <b>two refreshes over two different time periods</b>: <b>R1 = January 2026</b> "
         "and <b>R2 = February 2026</b>, the two months right after the provided 2023–2025 history. One calendar month per refresh is "
         "the natural unit for this business: hospital demand is reported monthly, so each refresh adds exactly one new hospital "
         "snapshot (12 hospitals × active medicines), and every monthly KPI gains a new data point."),
       P("Volumes are not arbitrary: each source's monthly volume is <b>measured from the history itself</b> (average of the last "
         "12 months, or of the last 92 days of normal sales) and multiplied by a "
         f"{(YOY_GROWTH - 1) * 100:.0f}% year-on-year growth factor (history: hospital consumption +3.5%/yr, sales +8%/yr). "
         "That gives ~25 k rows per refresh, about <b>2.4% of the history</b>: enough to visibly move every DE dashboard and "
         "every business KPI, small enough that trends remain realistic and the history stays dominant.")]
s2 += [P("Method", H2), bullets([
    "<b>Statistical bootstrap with noise:</b> rows are resampled from recent history (same calendar month last year where "
    "seasonality matters), dates are shifted into the target month and quantities get ±4–5% multiplicative noise.",
    "<b>Lineage preserved end-to-end:</b> new batches (02) → shipment lines (03, new GS1 SSCC-18 with check digit) → warehouse "
    "lots with a daily stock simulation (04, opening = previous closing) → sales (05) re-linked to a lot that was physically "
    "on hand and not expired on the sale date. Deliveries still in transit at month end become lots in the next refresh.",
    "<b>Business identities recomputed, not copied:</b> net = gross − discount; COGS = qty × wholesale; received = shipped − "
    "damaged − short; OTIF = on-time AND in-full; hospital closing stock = opening + received − dispensed − wastage, with "
    "unfulfilled demand when stock runs out; forecasts keep the ~1% under-forecast bias that the SDI mart is built to expose.",
    "<b>Year-start bulk postings are excluded</b> from the sales template so refreshes reflect real daily demand (~340 sales/day).",
    "<b>SCD triggers:</b> R1 re-prices 2 medicines; R2 re-prices 3, renames 1 brand (Type 1), launches 2 medicines "
    "(Tirzepatide, Apixaban) and expands HOSP003 from 310 to 360 beds (Type-2 history in dim_hospital).",
    f"<b>Data-quality injection:</b> dirty-row rates {', '.join(f'{k[:2]}={v:.1%}' for k, v in DIRTY_RATE.items())} (zero price, "
    "unknown/NULL SKU, invalid date, amount mismatch, duplicate key, negative quantity, sensor spike) to exercise validation.",
    "<b>Reproducible:</b> seeded by the refresh month; outputs are Excel files (batch → landing/, sales → stream_queue/).",
])]
gp = gen.pivot_table(index="source_name", columns="refresh_tag", values=["rows_generated", "dirty_rows"], aggfunc="sum")
base = q("SELECT source_name, rows_read FROM ops.batch_load_log WHERE refresh_tag='BASE'").set_index("source_name").rows_read
rows = [["Source file", "BASE rows", "R1 rows", "R1 dirty", "R2 rows", "R2 dirty", "Ingest path"]]
for s in gp.index:
    rows.append([s, f"{int(base.get(s, 0)):,}", f"{int(gp.loc[s, ('rows_generated', 'R1')]):,}", int(gp.loc[s, ("dirty_rows", "R1")]),
                 f"{int(gp.loc[s, ('rows_generated', 'R2')]):,}", int(gp.loc[s, ("dirty_rows", "R2")]),
                 "stream (API)" if s.startswith("05") else "batch (watcher)"])
rows.append(["Total", f"{int(base.sum()):,}", f"{int(gp[('rows_generated', 'R1')].sum()):,}", int(gp[("dirty_rows", "R1")].sum()),
             f"{int(gp[('rows_generated', 'R2')].sum()):,}", int(gp[("dirty_rows", "R2")].sum()), ""])
s2 += [P("Statistics of the generated data (from ops.generator_log)", H2), tbl(rows, [5.0, 2.2, 2.0, 1.7, 2.0, 1.7, 3.0], font=9),
       P("Master data (01) is a full snapshot per refresh; unchanged rows are skipped by staging as duplicates, so only the changed "
         "versions load. IoT and warehouse volumes follow the device and lot simulations (24 zone loggers every 3 h; lots on hand "
         "× days).", SMALL)]
s2 += [P("Effect on the pipeline", H2),
       P(f"After both refreshes: fact_sales grew from 500,000 to {fs[0]:,} rows (net sales ${fs[1]:,.0f}), fact_hospital_demand "
         f"from 27,216 to {fh:,} rows; dim_medicine holds 5 price versions and 2 new medicines; dim_hospital holds 2 versions of "
         "HOSP003. All injected dirty rows were rejected with reason codes, and staging = mart reconciliation passed after every refresh.")]
doc2 = SimpleDocTemplate(str(ROOT / "docs" / "Oversampling.Report.pdf"), pagesize=A4, leftMargin=1.8 * cm, rightMargin=1.8 * cm,
                         topMargin=1.4 * cm, bottomMargin=1.6 * cm, title="Oversampling Report - SDI Phase 1")
FOOTER[0] = "Oversampling Report — SDI Phase 1 · Data Analytics & Warehousing, Fall 2026"
doc2.build(s2, onFirstPage=footer, onLaterPages=footer)
print("reports written")
