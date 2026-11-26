"""Builds docs/Phase.1.BI.Report.pdf (max 5 pages) from the live BI state + per-refresh Superset screenshots."""
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, ListFlowable, ListItem, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
                                TableStyle)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from sdi.config import PG_DSN

FIG = ROOT / "docs" / "figures"
SHOTS = ROOT / "docs" / "bi_screenshots"
CROPS = FIG / "bi_crops"
CROPS.mkdir(parents=True, exist_ok=True)
pg = psycopg2.connect(PG_DSN)
q = lambda s: pd.read_sql(s, pg)

ss = getSampleStyleSheet()
NAVY = colors.HexColor("#1F3864")
BODY = ParagraphStyle("b", parent=ss["Normal"], fontName="Helvetica", fontSize=11, leading=13.4, alignment=TA_JUSTIFY, spaceAfter=3)
SMALL = ParagraphStyle("s", parent=BODY, fontSize=8.5, leading=10.3, textColor=colors.HexColor("#404040"))
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=16.5, leading=20, textColor=NAVY, spaceAfter=2)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=12.5, textColor=NAVY, spaceAfter=2, spaceBefore=5)
BUL = ParagraphStyle("bul", parent=BODY, alignment=0, spaceAfter=1)
P = lambda t, st=BODY: Paragraph(t, st)


def bullets(items):
    return ListFlowable([ListItem(P(i, BUL), leftIndent=10, value="•") for i in items], bulletType="bullet", start="•",
                        leftIndent=12, bulletFontSize=9)


def img(path, w_cm, max_h_cm=None):
    im = PILImage.open(path)
    w = w_cm * cm
    h = w * im.height / im.width
    if max_h_cm and h > max_h_cm * cm:
        h = max_h_cm * cm
        w = h * im.width / im.height
    return Image(str(path), width=w, height=h)


def crop(tag, slug, top, bottom, name):
    src = SHOTS / tag / f"{slug}.png"
    out = CROPS / f"{name}.png"
    im = PILImage.open(src)
    im.crop((0, top, im.width, min(bottom, im.height))).save(out)
    return out


def tbl(rows, widths, font=8.5, header=True):
    cell = ParagraphStyle("c", parent=BODY, fontSize=font, leading=font + 1.8, alignment=0, spaceAfter=0)
    head = ParagraphStyle("hd", parent=cell, fontName="Helvetica-Bold", textColor=colors.white)
    data = [[Paragraph(str(v), head if (i == 0 and header) else cell) for v in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=[w * cm for w in widths], repeatRows=1)
    t.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2F5597")),
                           ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#BFBFBF")), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                           ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F2F2")]),
                           ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                           ("TOPPADDING", (0, 0), (-1, -1), 1.3), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.3)]))
    return t


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#7F7F7F"))
    canvas.drawString(1.5 * cm, 0.9 * cm, "Phase.1.BI.Report — SDI BI layer · Data Analytics & Warehousing, Fall 2026")
    canvas.drawRightString(A4[0] - 1.5 * cm, 0.9 * cm, f"Page {doc.page}")
    canvas.restoreState()


# ------------------------------------------------------------------------------------------------ live numbers
k = q("SELECT refresh_tag, kpi_name, kpi_value FROM sdi_bi.bi_kpi_history")
K = {(r.refresh_tag, r.kpi_name): r.kpi_value for r in k.itertuples()}
kv = lambda n, t="R2": K.get((t, n), float("nan"))
rl = q("SELECT refresh_tag, data_as_of, fact_sales_rows, fact_hospital_rows, medicines, rows_published, publish_seconds "
       "FROM ops.bi_refresh_log ORDER BY refresh_id")
perf = q("SELECT count(*) n, avg(exec_ms) ms, count(*) FILTER (WHERE status<>'SUCCESS') bad FROM ops.query_log WHERE caller='bi_dashboard'").iloc[0]
gm_file = ROOT / "docs" / "group_members.csv"
gm = pd.read_csv(gm_file, dtype=str).fillna("") if gm_file.exists() else pd.DataFrame(columns=["name"])
gm = gm[gm["name"].str.strip() != ""] if len(gm) else gm
group = "; ".join(f"{r['name']} ({r.get('student_id', '')})" for _, r in gm.iterrows()) or "see README.md"

# crops (top part of each dashboard after R2: banner, KPI cards, main visuals, story)
c1 = crop("R2", "sdi-bi-1-hospital", 0, 1340, "bi1_r2")
c2 = crop("R2", "sdi-bi-2-channel", 0, 1420, "bi2_r2")
c3 = crop("R2", "sdi-bi-3-velocity", 0, 960, "bi3_r2")
c4 = crop("R2", "sdi-bi-4-unified", 0, 1000, "bi4_r2")
im4 = PILImage.open(SHOTS / "R2" / "sdi-bi-4-unified.png")
pivot_top = 1830
cpiv = crop("R2", "sdi-bi-4-unified", pivot_top, pivot_top + 700, "bi4_pivot_r2")

story = []
story += [P("Phase 1 BI Report — Sales &amp; Demand Intelligence (SDI) Dashboards", H1),
          P(f"Milestone 2: complete pipeline with BI and refreshing · Group: {group}", SMALL), Spacer(1, 3)]
story += [P("1. The problem the BI layer solves", H2),
          P("<b>Planners cannot see true medicine demand.</b> Pharmacy POS sales, e-commerce orders and hospital ERP demand "
            "arrive in three systems at three grains, so hospital forecasts under-shoot consumption, demand is lost between "
            "request and receipt, and production is planned on incomplete signals. The SDI mart (Milestone 1) unified these "
            "streams; the BI layer turns it into <b>four interactive Apache Superset dashboards</b> that make each part of the "
            "problem visible and actionable, and that <b>refresh automatically</b> whenever new data reaches the warehouse.")]
story += [P("2. BI architecture and automated refresh", H2), img(FIG / "bi_architecture.png", 17.6),
          P("Figure 1 — Medallion flow up to the BI layer and the Airflow DAG that refreshes it.", SMALL)]
story += [bullets([
    "<b>Serving layer, not the DuckDB file.</b> DuckDB allows one writer and blocks other processes while Airflow writes, so "
    "<i>publish_bi_mart</i> copies the gold mart (12 star tables + 7 BI tables) into a new Postgres schema and swaps it in "
    "one transaction: dashboards never see a half-loaded refresh. Superset reads it as a read-only <i>bi_reader</i> role.",
    "<b>Dashboards as code.</b> <i>sdi/bi/superset.py</i> defines 9 datasets with saved metrics, 47 charts and 4 dashboards "
    "with filter bars, cross-filtering and drill-to-detail; <i>sync_superset</i> re-applies them idempotently on every refresh.",
    "<b>Always current.</b> Results are never cached and dashboards re-query every 30 s. <i>sdi_06_bi_refresh</i> runs after "
    "every transform and as the last step of the one-click <i>sdi_99_refresh_cycle</i>; it snapshots 21 headline KPIs and "
    f"times every chart through Superset ({int(perf.n)} fetches, avg {perf.ms:.0f} ms, {int(perf.bad)} failures), which feeds "
    "the Grafana query dashboard.",
])]
story.append(PageBreak())

# ---------------------------------------------------------------- BI-1 + BI-2
story += [P("3. The dashboards and the story they tell", H2),
          P("3.1 BI-1 · Hospital Forecast vs Actual &amp; Unfulfilled Demand", H2), img(c1, 16.5, 9.6),
          P(f"<b>Story:</b> forecasts under-shoot real consumption (bias <b>{kv('Forecast bias %'):.2f}%</b>, negative in every "
            f"disease category; error {kv('Forecast error (WAPE) %'):.2f}%). Only <b>{kv('Request-to-receipt fill rate %'):.1f}%</b> of "
            f"requested packs arrive — a <b>{kv('Supply gap (packs)'):,.0f}-pack</b> supply gap — and {kv('Unfulfilled demand (packs)'):,.0f} "
            "packs went unserved. The funnel shows where packs are lost, the heatmap and hotspot table show which hospital and "
            "therapeutic class to fix first. <b>Solves:</b> turns the hidden forecast gap into a ranked action list.")]
story += [P("3.2 BI-2 · Channel Revenue Split &amp; Gross Margin", H2), img(c2, 16.5, 9.6),
          P(f"<b>Story:</b> Retail carries ~60% of revenue, hospital pharmacies {kv('Hospital-pharmacy share of revenue %'):.1f}% "
            f"and e-commerce {kv('E-commerce share of revenue %'):.1f}% (growing fastest). Gross margin is flat at "
            f"<b>{kv('Gross margin %'):.1f}%</b> in every channel, so channel strategy is a volume story, and "
            f"<b>{kv('E-commerce returned-to-sender %'):.0f}%</b> of e-commerce orders are returned to sender — fix fulfilment "
            "before pushing online volume. Revenue peaks every December. <b>Solves:</b> shows planners which channel drives demand.")]
story.append(PageBreak())

# ---------------------------------------------------------------- BI-3 + BI-4
story += [P("3.3 BI-3 · Fast vs Slow-moving Medicines (Cold Chain)", H2), img(c3, 16.5, 7.4),
          P(f"<b>Story:</b> velocity = packs per active month. The three cold-chain medicines are only "
            f"<b>{kv('Cold-chain share of packs %'):.1f}%</b> of packs but <b>{kv('Cold-chain share of revenue %'):.1f}%</b> of "
            "revenue and are among the slowest movers, the costliest items to over- or under-produce; the treemap, bubble chart "
            "and league table rank every medicine within its therapeutic class, and the two new launches (Tirzepatide, "
            "Apixaban) appear after refresh R2. <b>Solves:</b> tells production what to make in small, frequent batches and "
            "which fast movers to protect from stock-outs.")]
story += [P("3.4 BI-4 · Unified Demand &amp; Refresh Tracker", H2), img(c4, 16.5, 7.6),
          P(f"<b>Story:</b> the single demand signal the company lacked: sales channels and hospital wards on conformed "
            f"dimensions. Hospital wards are <b>{kv('Hospital-ward share of unified demand %'):.0f}%</b> of all packs — sales data "
            "alone hides most real demand — and requested-vs-received shows the supply gap month by month. Its KPI-by-refresh "
            "table and refresh log prove that the warehouse and every dashboard changed after each refresh. "
            "<b>Solves:</b> planning moves from partial sales signals to total demand.")]
story.append(PageBreak())

# ---------------------------------------------------------------- refresh evidence
story += [P("4. Refreshing the data mart and the dashboards (twice)", H2),
          P("Each refresh adds one month of generated data (R1 = January 2026, R2 = February 2026). One trigger of "
            "<i>sdi_99_refresh_cycle</i> runs generator → batch + stream ingestion → transform → query layer → BI refresh; "
            "within 30 seconds every dashboard shows the new data. KPIs are snapshotted after each refresh:", BODY),
          img(FIG / "bi_refresh_kpis.png", 17.4)]
rows = [["Refresh", "Data as of", "Sales rows", "Hospital rows", "Medicines", "Rows published to sdi_bi", "Publish (s)"]]
for r in rl.itertuples():
    rows.append([r.refresh_tag, str(r.data_as_of), f"{r.fact_sales_rows:,}", f"{r.fact_hospital_rows:,}", r.medicines,
                 f"{r.rows_published:,}", f"{float(r.publish_seconds):.1f}"])
story += [tbl(rows, [2.0, 2.4, 2.4, 2.6, 2.0, 3.6, 2.0]), Spacer(1, 4)]
chg = [["What changed on screen", "BASE", "R1", "R2"],
       ["Latest month on every time axis", str(rl.data_as_of.iloc[0])[:7], str(rl.data_as_of.iloc[1])[:7], str(rl.data_as_of.iloc[2])[:7]],
       ["Supply gap (BI-1)", f"{kv('Supply gap (packs)', 'BASE'):,.0f}", f"{kv('Supply gap (packs)', 'R1'):,.0f}", f"{kv('Supply gap (packs)'):,.0f}"],
       ["Latest-month forecast bias (BI-1)", f"{kv('Latest month forecast bias %', 'BASE'):.2f}%", f"{kv('Latest month forecast bias %', 'R1'):.2f}%", f"{kv('Latest month forecast bias %'):.2f}%"],
       ["Net revenue, normal postings (BI-2)", f"${kv('Net revenue USD (normal postings)', 'BASE'):,.0f}", f"${kv('Net revenue USD (normal postings)', 'R1'):,.0f}", f"${kv('Net revenue USD (normal postings)'):,.0f}"],
       ["Medicines on sale (BI-3)", f"{kv('Medicines on sale', 'BASE'):.0f}", f"{kv('Medicines on sale', 'R1'):.0f}", f"{kv('Medicines on sale'):.0f} (2 launches)"],
       ["Unified demand packs (BI-4)", f"{kv('Unified demand packs (all channels incl. wards)', 'BASE'):,.0f}", f"{kv('Unified demand packs (all channels incl. wards)', 'R1'):,.0f}", f"{kv('Unified demand packs (all channels incl. wards)'):,.0f}"]]
story += [tbl(chg, [7.2, 3.3, 3.3, 3.3], font=8.6), Spacer(1, 4),
          P("Figure — BI-4 after R2: the KPI-by-refresh pivot shows BASE, R1 and R2 side by side; the refresh banner on top of "
            "every dashboard shows the refresh tag and data-as-of date.", SMALL), img(cpiv, 13.5, 7.0)]
story.append(PageBreak())

# ---------------------------------------------------------------- insights + assumptions
story += [P("5. From insight to action", H2),
          tbl([["Dashboard", "Insight", "Action"],
               ["BI-1", f"Under-forecast in all categories (bias {kv('Forecast bias %'):.2f}%)", "Apply the measured bias as a forecast correction; review monthly"],
               ["BI-1", f"{100 - kv('Request-to-receipt fill rate %'):.1f}% of requested packs never arrive", "Attack approval cuts first, then delivery shortfalls"],
               ["BI-1", "Unfulfilled / emergency demand concentrated in a few hospital × class pairs", "Priority allocation + safety stock for the hotspots"],
               ["BI-2", "Margin flat; e-commerce growing; 1 in 4 e-com orders returned", "Fix e-commerce fulfilment before scaling it"],
               ["BI-3", "Cold-chain SKUs: high value, slow moving", "Small, frequent cold-chain batches; expiry watch (Phase 2/3)"],
               ["BI-4", "Hospital wards ≈ 92% of demand", "Plan production on the unified demand signal, not on sales"]],
              [1.8, 7.6, 8.2], font=8.6)]
story += [P("6. Assumptions", H2), bullets([
    "Apache Superset is the BI tool (open source, Docker, runs on macOS where Power BI Desktop does not).",
    "BI reads the Postgres serving copy <i>sdi_bi</i>, refreshed after every transform; dashboards are never cached and "
    "auto-refresh every 30 s.",
    "Year-start bulk postings (22% of sales) are excluded from revenue trends by default (filter <i>Date quality = Normal</i>).",
    "Velocity = packs per active month, so new launches are compared fairly; ratios are always computed from sums so they "
    "stay correct under any filter.",
    "Channels: Retail = chain + independent pharmacies, Hospital pharmacy, E-commerce, and Hospital ward (ERP consumption, "
    "unified demand only). Margin is identical by channel in the source data.",
    "Each refresh adds one month (~25 k rows); 'data as of' is the latest sale date in the warehouse.",
])]
story += [P("7. Running it", H2),
          P("<i>docker compose up -d --build</i> starts Postgres, Airflow, the ingestion API, the file-watcher, Grafana and "
            "Superset (http://localhost:8088, admin/admin). Trigger <i>sdi_99_refresh_cycle</i> with BASE, then R1, then R2; "
            "the four dashboards are created automatically on the first refresh. Full detail, KPI dictionary, chart catalogue, "
            "refresh evidence and an interactive Excel twin of the dashboards are in <b>SDI.Excel.BI.xlsx</b>.", BODY)]

doc = SimpleDocTemplate(str(ROOT / "docs" / "Phase.1.BI.Report.pdf"), pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm,
                        topMargin=1.2 * cm, bottomMargin=1.5 * cm, title="Phase.1.BI.Report - SDI BI layer")
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print("report written")
