"""SDI BI layer in Apache Superset — dashboards as code.

    python -m sdi.bi.superset provision     # create/update database, datasets, charts, 4 dashboards (idempotent)
    python -m sdi.bi.superset warm          # refresh every chart through Superset's API, timed -> ops.query_log
    python -m sdi.bi.superset refresh       # re-sync dataset columns after a publish

Everything the dashboards show comes from the Postgres serving schema `sdi_bi` (see sdi/bi/publish.py), read through a
least-privilege `bi_reader` role. Results are never cached (NullCache) and dashboards auto-refresh every 30 s, so a
pipeline refresh shows up on screen without any manual step.
"""
import json
import os
import sys
import time
import uuid
import datetime as dt

import requests

SUPERSET_URL = os.getenv("SDI_SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.getenv("SDI_SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.getenv("SDI_SUPERSET_PASSWORD", "admin")
BI_DB_NAME = "SDI BI (sdi_bi serving layer)"
BI_DB_URI = os.getenv("SDI_BI_SQLALCHEMY_URI", "postgresql+psycopg2://bi_reader:bi_reader@postgres:5432/pharma_sdi")
SCHEMA = "sdi_bi"
REFRESH_SECONDS = 30


# =============================================================================================== API client
class Superset:
    def __init__(self, url=SUPERSET_URL, user=SUPERSET_USER, password=SUPERSET_PASSWORD):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(f"{self.url}/api/v1/security/login",
                        json={"username": user, "password": password, "provider": "db", "refresh": True}, timeout=30)
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        try:
            tok = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=30).json().get("result")
            if tok:
                self.s.headers["X-CSRFToken"] = tok
                self.s.headers["Referer"] = self.url
        except Exception:
            pass

    def call(self, method, path, **kw):
        r = self.s.request(method, f"{self.url}/api/v1/{path}", timeout=120, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:600]}")
        return r.json() if r.text else {}

    def find(self, resource, col, value):
        v = str(value).replace("!", "!!").replace("'", "!'")
        q = f"(filters:!((col:{col},opr:eq,value:'{v}')),page_size:100)"
        res = self.call("GET", f"{resource}/", params={"q": q}).get("result", [])
        return res[0]["id"] if res else None


# =============================================================================================== datasets & metrics
def M(name, expr, fmt=None, label=None, desc=None):
    d = {"metric_name": name, "expression": expr, "verbose_name": label or name.replace("_", " ").title()}
    if fmt:
        d["d3format"] = fmt
    if desc:
        d["description"] = desc
    return d


PCT = ".1%"
NUM = ",.0f"
USD = "$,.0f"
DATASETS = {
    "bi_hospital_demand": dict(dttm="month_date", desc="Hospital x medicine x month demand snapshot (fact_hospital_demand + dims)",
        metrics=[M("forecast_packs", "SUM(forecast_packs)", NUM, "Forecast (packs)"),
                 M("consumed_packs", "SUM(consumed_packs)", NUM, "Actual consumption (packs)"),
                 M("requested_packs", "SUM(requested_packs)", NUM, "Requested (packs)"),
                 M("received_packs", "SUM(received_packs)", NUM, "Received (packs)"),
                 M("forecast_bias_pct", "SUM(forecast_error_packs)*1.0/NULLIF(SUM(consumed_packs),0)", PCT, "Forecast bias",
                   "(forecast - consumed) / consumed; negative = under-forecast"),
                 M("wape_pct", "SUM(abs_error_packs)*1.0/NULLIF(SUM(consumed_packs),0)", PCT, "Forecast error (WAPE)"),
                 M("forecast_ratio", "SUM(forecast_packs)*1.0/NULLIF(SUM(consumed_packs),0)", ".3f", "Forecast / actual ratio"),
                 M("fill_rate_pct", "SUM(received_packs)*1.0/NULLIF(SUM(requested_packs),0)", PCT, "Request-to-receipt fill rate"),
                 M("supply_gap_packs", "SUM(requested_packs - received_packs)", NUM, "Supply gap (packs)"),
                 M("unfulfilled_packs", "SUM(unfulfilled_packs)", NUM, "Unfulfilled demand (packs)"),
                 M("emergency_packs", "SUM(emergency_packs)", NUM, "Emergency requests (packs)"),
                 M("emergency_pct", "SUM(emergency_packs)*1.0/NULLIF(SUM(consumed_packs),0)", PCT, "Emergency demand share"),
                 M("stockout_days", "SUM(stockout_days)", NUM, "Stock-out days"),
                 M("service_level_pct", "1 - SUM(unfulfilled_packs)*1.0/NULLIF(SUM(dispensed_packs)+SUM(unfulfilled_packs),0)",
                   ".2%", "Service level")]),
    "bi_supply_funnel": dict(dttm="month_date", desc="Request -> approval -> receipt -> consumption funnel (long format)",
        metrics=[M("packs", "SUM(packs)", NUM, "Packs")]),
    "bi_sales_monthly": dict(dttm="month_date", desc="Monthly sales cube: medicine x channel x state x promotion x segment",
        metrics=[M("net_revenue", "SUM(net_usd)", USD, "Net revenue"),
                 M("gross_revenue", "SUM(gross_usd)", USD, "Gross revenue"),
                 M("margin_usd", "SUM(margin_usd)", USD, "Gross margin $"),
                 M("gross_margin_pct", "SUM(margin_usd)*1.0/NULLIF(SUM(net_usd),0)", PCT, "Gross margin %"),
                 M("packs", "SUM(packs)", NUM, "Packs sold"),
                 M("txns", "SUM(txn_count)", NUM, "Transactions"),
                 M("avg_packs_per_txn", "SUM(packs)*1.0/NULLIF(SUM(txn_count),0)", ".2f", "Avg packs / transaction"),
                 M("discount_usd", "SUM(discount_usd)", USD, "Discounts $"),
                 M("discount_pct", "SUM(discount_usd)*1.0/NULLIF(SUM(gross_usd),0)", ".2%", "Discount rate"),
                 M("rts_pct", "SUM(rts_txn)*1.0/NULLIF(SUM(ecom_orders),0)", PCT, "Returned-to-sender rate"),
                 M("promoted_share", "SUM(promoted_txn)*1.0/NULLIF(SUM(txn_count),0)", PCT, "Promoted share"),
                 M("avg_days_to_expiry", "SUM(days_to_expiry_sum)*1.0/NULLIF(SUM(txn_count),0)", ",.0f",
                   "Avg days to expiry at sale")]),
    "bi_medicine_velocity": dict(dttm=None, desc="One row per medicine: velocity (packs / active month), rank, class",
        metrics=[M("packs_per_month", "SUM(packs_per_month)", ",.1f", "Packs per month"),
                 M("net_per_month", "SUM(net_per_month)", USD, "Net revenue per month"),
                 M("net_usd", "SUM(net_usd)", USD, "Net revenue"),
                 M("margin_usd", "SUM(margin_usd)", USD, "Gross margin $"),
                 M("medicines", "COUNT(*)", NUM, "Medicines"),
                 M("avg_days_to_expiry", "AVG(avg_days_to_expiry)", ",.0f", "Avg days to expiry at sale"),
                 M("cold_chain_revenue_share", "SUM(CASE WHEN cold_chain_flag='Y' THEN net_usd ELSE 0 END)*1.0/NULLIF(SUM(net_usd),0)",
                   PCT, "Cold-chain share of revenue"),
                 M("cold_chain_pack_share", "SUM(CASE WHEN cold_chain_flag='Y' THEN packs ELSE 0 END)*1.0/NULLIF(SUM(packs),0)",
                   ".2%", "Cold-chain share of packs")]),
    "bi_unified_demand": dict(dttm="month_date", desc="Unified demand: sales channels + hospital wards on conformed dims",
        metrics=[M("demand_packs", "SUM(demand_packs)", NUM, "Demand (packs)"),
                 M("net_usd", "SUM(net_usd)", USD, "Net revenue")]),
    "bi_demand_supply_monthly": dict(dttm="month_date", desc="Monthly demand vs supply per region x disease",
        metrics=[M("commercial_packs", "SUM(commercial_packs)", NUM, "Commercial sales (packs)"),
                 M("ward_consumed_packs", "SUM(ward_consumed_packs)", NUM, "Hospital ward consumption (packs)"),
                 M("forecast_packs", "SUM(forecast_packs)", NUM, "Hospital forecast (packs)"),
                 M("requested_packs", "SUM(requested_packs)", NUM, "Hospital requested (packs)"),
                 M("received_packs", "SUM(received_packs)", NUM, "Hospital received (packs)"),
                 M("unfulfilled_packs", "SUM(unfulfilled_packs)", NUM, "Unfulfilled (packs)"),
                 M("emergency_packs", "SUM(emergency_packs)", NUM, "Emergency (packs)")]),
    "bi_recent_sales": dict(dttm="sale_date", desc="Newest 1,000 sales transactions (shows streamed refresh rows)", metrics=[]),
    "bi_refresh_history": dict(dttm="published_at", desc="One row per BI publish (refresh log)",
        metrics=[M("latest_sales_rows", "MAX(fact_sales_rows)", NUM, "Sales transactions in warehouse"),
                 M("publishes", "COUNT(*)", NUM, "BI publishes")]),
    "bi_kpi_history": dict(dttm=None, desc="Headline KPIs snapshotted after every refresh (latest per refresh tag)",
        metrics=[M("kpi_value", "MAX(kpi_value)", ",.2f", "KPI value")]),
}


# =============================================================================================== chart specs
def F(col, op, val):
    """Simple adhoc filter."""
    return {"expressionType": "SIMPLE", "subject": col, "operator": op, "comparator": val, "clause": "WHERE",
            "isExtra": False, "filterOptionName": f"f_{uuid.uuid4().hex[:10]}"}


NORMAL = F("date_quality_flag", "==", "Normal")
COMMON = {"time_range": "No filter", "row_limit": 10000, "show_legend": True, "rich_tooltip": True,
          "color_scheme": "supersetColors", "legendOrientation": "top", "legendType": "scroll"}


def big(name, ds, metric, sub, fmt=None, filters=()):
    return dict(name=name, ds=ds, viz="big_number_total",
                fd={"metric": metric, "subheader": sub, "header_font_size": 0.4, "subheader_font_size": 0.125,
                    "y_axis_format": fmt or "SMART_NUMBER", "adhoc_filters": list(filters)})


def ts(name, ds, viz, metrics, groupby=(), filters=(), fmt="SMART_NUMBER", extra=None):
    fd = {"x_axis": "month_date", "time_grain_sqla": "P1M", "metrics": list(metrics), "groupby": list(groupby),
          "adhoc_filters": list(filters), "y_axis_format": fmt, "x_axis_time_format": "%Y-%m",
          "truncateYAxis": False, "zoomable": True, "markerEnabled": True, "markerSize": 4}
    fd.update(extra or {})
    return dict(name=name, ds=ds, viz=viz, fd=fd)


def catbar(name, ds, x, metrics, groupby=(), filters=(), fmt="SMART_NUMBER", horizontal=False, stack=False,
           sort_metric=None, asc=False, limit=10000, extra=None):
    fd = {"x_axis": x, "metrics": list(metrics), "groupby": list(groupby), "adhoc_filters": list(filters),
          "y_axis_format": fmt, "orientation": "horizontal" if horizontal else "vertical", "row_limit": limit,
          "show_value": True, "x_axis_sort_asc": asc, "truncateYAxis": False}
    if sort_metric:
        fd["x_axis_sort"] = sort_metric
    if stack:
        fd["stack"] = "Stack"
    fd.update(extra or {})
    return dict(name=name, ds=ds, viz="echarts_timeseries_bar", fd=fd)


def table(name, ds, groupby, metrics, filters=(), limit=10, order=None, extra=None):
    fd = {"query_mode": "aggregate", "groupby": list(groupby), "metrics": list(metrics), "adhoc_filters": list(filters),
          "row_limit": limit, "order_desc": True, "show_cell_bars": True, "table_timestamp_format": "%Y-%m-%d",
          "include_search": False, "allow_rearrange_columns": False}
    if order:
        fd["timeseries_limit_metric"] = order
    fd.update(extra or {})
    return dict(name=name, ds=ds, viz="table", fd=fd)


def raw_table(name, ds, cols, order_col, limit=15, filters=()):
    return dict(name=name, ds=ds, viz="table", fd={
        "query_mode": "raw", "all_columns": list(cols), "order_by_cols": [json.dumps([order_col, False])],
        "row_limit": limit, "adhoc_filters": list(filters), "table_timestamp_format": "%Y-%m-%d %H:%M",
        "include_search": False, "show_cell_bars": False})


REFRESH_BANNER = raw_table("SDI · Data refresh status", "bi_refresh_history",
                           ["refresh_tag", "data_as_of", "published_at", "fact_sales_rows", "fact_hospital_rows",
                            "medicines"], "published_at", limit=3)

DASHBOARDS = [
    dict(slug="sdi-bi-1-hospital", title="SDI · BI-1 · Hospital Forecast vs Actual & Unfulfilled Demand",
         story=("### How to read this dashboard\n"
                "**Question:** are hospital forecasts keeping up with real consumption, and where is demand lost?\n\n"
                "* **Forecast bias** below 0 means hospitals *under-forecast* — they consume more than they planned. "
                "It is negative in every disease category.\n"
                "* The **funnel** shows packs lost between *requested → approved → received*: that gap is demand the "
                "supply chain never served.\n"
                "* The **heatmap** and **hotspot table** point to *which hospital and therapeutic class* to fix first.\n\n"
                "**Action:** raise forecasts where bias is most negative; prioritise supply to the hotspot hospitals. "
                "Each refresh adds a month — watch the latest bar of the bias chart."),
         filters=[("Time range", "time", None), ("Region", "census_region", "bi_hospital_demand"),
                  ("Hospital", "hospital_name", "bi_hospital_demand"),
                  ("Disease category", "disease_category", "bi_hospital_demand")],
         rows=[
             [(REFRESH_BANNER, 12, 22)],
             [(big("Forecast bias", "bi_hospital_demand", "forecast_bias_pct", "forecast vs actual (− = under-forecast)", ".2%"), 2, 26),
              (big("Forecast error (WAPE)", "bi_hospital_demand", "wape_pct", "weighted abs. % error", ".2%"), 2, 26),
              (big("Fill rate", "bi_hospital_demand", "fill_rate_pct", "received / requested", ".1%"), 2, 26),
              (big("Supply gap (packs)", "bi_hospital_demand", "supply_gap_packs", "requested − received"), 2, 26),
              (big("Unfulfilled demand (packs)", "bi_hospital_demand", "unfulfilled_packs", "patients not served"), 2, 26),
              (big("Emergency demand", "bi_hospital_demand", "emergency_pct", "emergency packs / consumption", ".2%"), 2, 26)],
             [(ts("Forecast vs actual consumption (monthly)", "bi_hospital_demand", "echarts_timeseries_line",
                  ["forecast_packs", "consumed_packs"]), 7, 55),
              ("MARKDOWN", 5, 55)],
             [(ts("Forecast bias by month (− = under-forecast)", "bi_hospital_demand", "echarts_timeseries_bar",
                  ["forecast_bias_pct"], fmt=".1%"), 6, 50),
              (dict(name="Request → approval → receipt → consumption funnel", ds="bi_supply_funnel", viz="funnel",
                    fd={"groupby": ["stage"], "metric": "packs", "sort_by_metric": False, "label_type": "key_value_percent",
                        "number_format": "SMART_NUMBER", "show_labels": True, "legendOrientation": "top", "show_legend": False,
                        "adhoc_filters": []}), 6, 50)],
             [(dict(name="Forecast error (WAPE) by hospital x disease", ds="bi_hospital_demand", viz="heatmap_v2",
                    fd={"x_axis": "disease_category", "groupby": "hospital_name", "metric": "wape_pct", "adhoc_filters": [],
                        "row_limit": 10000, "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc", "normalize_across": "heatmap",
                        "linear_color_scheme": "oranges", "show_values": True, "show_legend": True, "y_axis_format": ".1%",
                        "left_margin": "auto", "bottom_margin": "auto", "value_bounds": [None, None]}), 7, 70),
              (table("Unfulfilled & emergency hotspots (hospital x class)", "bi_hospital_demand",
                     ["hospital_name", "therapeutic_class"], ["unfulfilled_packs", "emergency_packs", "stockout_days"],
                     limit=12, order="unfulfilled_packs"), 5, 70)],
             [(catbar("Supply gap by hospital (packs)", "bi_hospital_demand", "hospital_name",
                      ["supply_gap_packs", "unfulfilled_packs"], horizontal=True, sort_metric="supply_gap_packs", asc=True), 12, 55)],
         ]),
    dict(slug="sdi-bi-2-channel", title="SDI · BI-2 · Channel Revenue Split & Gross Margin",
         story=("### How to read this dashboard\n"
                "**Question:** how do Retail, Hospital-pharmacy and E-commerce compare on revenue, growth and margin?\n\n"
                "* **Retail** carries most revenue; **E-commerce** grows fastest year on year.\n"
                "* **Gross margin is flat (~23%) in every channel** — channel growth is a *volume* story, not a pricing one.\n"
                "* Revenue peaks every **December** (seasonal), then drops in January.\n"
                "* ~1 in 4 e-commerce orders is **returned to sender** — a fulfilment problem worth fixing before "
                "pushing more volume online.\n\n"
                "Year-start bulk postings are excluded by default (filter *Date quality = Normal*). "
                "Each refresh extends the monthly trend by one month."),
         filters=[("Time range", "time", None), ("Region", "census_region", "bi_sales_monthly"),
                  ("Channel", "channel_group", "bi_sales_monthly"), ("Disease category", "disease_category", "bi_sales_monthly"),
                  ("Date quality", "date_quality_flag", "bi_sales_monthly")],
         default_filters={"date_quality_flag": ["Normal"]},
         rows=[
             [(REFRESH_BANNER, 12, 22)],
             [(big("Net revenue", "bi_sales_monthly", "net_revenue", "after discounts", "$,.0f"), 3, 26),
              (big("Gross margin %", "bi_sales_monthly", "gross_margin_pct", "margin / net revenue", ".2%"), 3, 26),
              (big("Transactions", "bi_sales_monthly", "txns", "sales transactions"), 3, 26),
              (big("E-commerce returned-to-sender", "bi_sales_monthly", "rts_pct", "of e-commerce orders", ".1%"), 3, 26)],
             [(ts("Monthly net revenue by channel", "bi_sales_monthly", "echarts_area", ["net_revenue"], ["channel_group"],
                  fmt="$,.0f", extra={"stack": "Stack", "opacity": 0.5, "markerEnabled": False}), 7, 55),
              ("MARKDOWN", 5, 55)],
             [(dict(name="Revenue split by channel", ds="bi_sales_monthly", viz="pie",
                    fd={"groupby": ["channel_group"], "metric": "net_revenue", "donut": True, "show_labels": True,
                        "label_type": "percent", "labels_outside": True, "number_format": "$,.0f", "innerRadius": 40,
                        "outerRadius": 75, "show_legend": True, "legendOrientation": "top", "adhoc_filters": [],
                        "row_limit": 100, "sort_by_metric": True}), 4, 55),
              (catbar("Net revenue by year and channel", "bi_sales_monthly", "year_label", ["net_revenue"], ["channel_group"],
                      fmt="$,.0f", extra={"legendType": "plain"}), 4, 55),
              (catbar("Gross margin % by channel and year", "bi_sales_monthly", "channel_group", ["gross_margin_pct"],
                      ["year_label"], fmt=".1%", extra={"sort_series_type": "name", "sort_series_ascending": True}), 4, 55)],
             [(catbar("Revenue by region and channel", "bi_sales_monthly", "census_region", ["net_revenue"], ["channel_group"],
                      fmt="$,.0f", stack=True), 6, 50),
              (ts("E-commerce returned-to-sender rate by month", "bi_sales_monthly", "echarts_timeseries_line", ["rts_pct"],
                  filters=[F("channel_group", "==", "E-commerce")], fmt=".1%"), 6, 50)],
             [(table("Promotion effectiveness", "bi_sales_monthly", ["promotion_type"],
                     ["txns", "avg_packs_per_txn", "discount_usd", "discount_pct", "gross_margin_pct"], limit=10,
                     order="txns"), 6, 45),
              (table("Revenue by disease category and channel", "bi_sales_monthly", ["disease_category", "channel_group"],
                     ["net_revenue", "packs", "gross_margin_pct"], limit=25, order="net_revenue"), 6, 45)],
         ]),
    dict(slug="sdi-bi-3-velocity", title="SDI · BI-3 · Fast vs Slow-moving Medicines (Cold Chain)",
         story=("### How to read this dashboard\n"
                "**Question:** which medicines move fast or slow inside each therapeutic class — especially cold-chain items?\n\n"
                "* **Velocity** = packs sold per active month (normal postings).\n"
                "* The 3 **cold-chain** medicines (insulins, GLP-1) are only ~2% of packs but ~14% of revenue and are among "
                "the slowest movers: the costliest items to over- or under-produce.\n"
                "* **New launches** (Tirzepatide, Apixaban) appear after refresh R2.\n\n"
                "**Action:** produce slow, high-value cold-chain medicines in smaller, more frequent batches; protect "
                "the fast movers from stock-outs."),
         filters=[("Therapeutic class", "therapeutic_class", "bi_medicine_velocity"),
                  ("Cold chain", "cold_chain_flag", "bi_medicine_velocity"),
                  ("Disease category", "disease_category", "bi_medicine_velocity"),
                  ("Velocity class", "velocity_class", "bi_medicine_velocity")],
         rows=[
             [(REFRESH_BANNER, 12, 22)],
             [(big("Medicines on sale", "bi_medicine_velocity", "medicines", "incl. new launches"), 3, 26),
              (big("Cold-chain share of revenue", "bi_medicine_velocity", "cold_chain_revenue_share", "3 cold-chain SKUs", ".1%"), 3, 26),
              (big("Cold-chain share of packs", "bi_medicine_velocity", "cold_chain_pack_share", "volume share", ".2%"), 3, 26),
              (big("Avg days to expiry at sale", "bi_sales_monthly", "avg_days_to_expiry", "shelf life left when sold", ",.0f",
                   filters=[NORMAL]), 3, 26)],
             [(catbar("Top 10 fast-moving medicines (packs / month)", "bi_medicine_velocity", "generic_name", ["packs_per_month"],
                      filters=[F("overall_rank", "<=", 10)], horizontal=True,
                      sort_metric="packs_per_month", asc=True, fmt=",.0f"), 6, 55),
              (catbar("Top 10 slow-moving medicines (packs / month)", "bi_medicine_velocity", "generic_name", ["packs_per_month"],
                      filters=[F("slow_rank", "<=", 10)], horizontal=True,
                      sort_metric="packs_per_month", asc=False, fmt=",.1f"), 6, 55)],
             [(dict(name="Velocity by therapeutic class (treemap)", ds="bi_medicine_velocity", viz="treemap_v2",
                    fd={"groupby": ["therapeutic_class", "generic_name"], "metric": "packs_per_month", "show_labels": True,
                        "show_upper_labels": True, "label_type": "key_value", "number_format": ",.0f",
                        "adhoc_filters": [], "row_limit": 1000}), 7, 60),
              ("MARKDOWN", 5, 60)],
             [(dict(name="Velocity vs revenue (bubble = margin, colour = cold chain)", ds="bi_medicine_velocity",
                    viz="bubble_v2",
                    fd={"entity": "generic_name", "x": "packs_per_month", "y": "net_per_month", "size": "margin_usd",
                        "series": "cold_chain_flag", "adhoc_filters": [], "row_limit": 1000, "max_bubble_size": "25",
                        "x_axis_label": "Packs per month (log)", "y_axis_label": "Net revenue per month",
                        "logXAxis": True, "logYAxis": False, "xAxisFormat": ",.0f", "y_axis_format": "$,.0f",
                        "show_legend": True, "legendOrientation": "top", "opacity": 0.7, "truncateYAxis": False}), 6, 60),
              (ts("Cold-chain medicines: monthly packs", "bi_sales_monthly", "echarts_timeseries_line", ["packs"],
                  ["generic_name"], filters=[F("cold_chain_flag", "==", "Y"), NORMAL]), 6, 60)],
             [(table("Medicine velocity league table", "bi_medicine_velocity",
                     ["generic_name", "therapeutic_class", "cold_chain_flag", "velocity_class", "lifecycle"],
                     ["packs_per_month", "net_per_month", "avg_days_to_expiry"], limit=70, order="packs_per_month"), 12, 70)],
         ]),
    dict(slug="sdi-bi-4-unified", title="SDI · BI-4 · Unified Demand & Refresh Tracker",
         story=("### The single demand signal\n"
                "Before the SDI mart, POS sales, e-commerce orders and hospital ERP demand lived in three systems at three "
                "grains. Here they are **one demand signal** on conformed medicine / date / geography / channel dimensions.\n\n"
                "* **Hospital wards** consume the vast majority of packs — sales data alone hides most real demand.\n"
                "* **Requested vs received** shows the supply gap month by month; emergency requests spike when it widens.\n"
                "* The **KPI-by-refresh** table and the **refresh log** prove the warehouse and these dashboards changed "
                "after each refresh (BASE → R1 → R2)."),
         filters=[("Time range", "time", None), ("Region", "census_region", "bi_unified_demand"),
                  ("Channel", "channel_group", "bi_unified_demand"),
                  ("Disease category", "disease_category", "bi_unified_demand")],
         rows=[
             [(REFRESH_BANNER, 12, 22)],
             [(big("Sales transactions in warehouse", "bi_refresh_history", "latest_sales_rows", "fact_sales rows (grows per refresh)"), 3, 26),
              (big("Unified demand (packs)", "bi_unified_demand", "demand_packs", "all channels + hospital wards"), 3, 26),
              (big("Hospital received (packs)", "bi_demand_supply_monthly", "received_packs", "supply delivered to hospitals"), 3, 26),
              (big("Unfulfilled (packs)", "bi_demand_supply_monthly", "unfulfilled_packs", "demand not served"), 3, 26)],
             [(ts("Unified monthly demand by channel (log scale)", "bi_unified_demand", "echarts_timeseries_line",
                  ["demand_packs"], ["channel_group"], extra={"logAxis": True}), 7, 55),
              ("MARKDOWN", 5, 55)],
             [(ts("Hospital demand vs supply (monthly)", "bi_demand_supply_monthly", "echarts_timeseries_line",
                  ["forecast_packs", "requested_packs", "received_packs", "ward_consumed_packs"]), 7, 55),
              (dict(name="Demand share: region → channel", ds="bi_unified_demand", viz="sunburst_v2",
                    fd={"columns": ["census_region", "channel_group"], "metric": "demand_packs", "adhoc_filters": [],
                        "row_limit": 1000, "show_labels": True, "label_type": "key", "number_format": "SMART_NUMBER",
                        "show_total": True}), 5, 55)],
             [(catbar("Commercial demand by region and channel (packs, % of region)", "bi_unified_demand", "census_region",
                      ["demand_packs"], ["channel_group"], filters=[F("channel_group", "!=", "Hospital ward")], stack=True,
                      extra={"contributionMode": "row"}, fmt=".0%"), 6, 50),
              (ts("Emergency & unfulfilled packs by month", "bi_demand_supply_monthly", "echarts_timeseries_bar",
                  ["emergency_packs", "unfulfilled_packs"]), 6, 50)],
             [(dict(name="Headline KPIs after each refresh (BASE → R1 → R2)", ds="bi_kpi_history", viz="pivot_table_v2",
                    fd={"groupbyRows": ["dashboard", "kpi_name"], "groupbyColumns": ["refresh_tag"], "metrics": ["kpi_value"],
                        "aggregateFunction": "Sum", "metricsLayout": "COLUMNS", "valueFormat": ",.2f", "rowOrder": "key_a_to_z",
                        "colOrder": "key_a_to_z", "adhoc_filters": [], "row_limit": 1000, "rowTotals": False,
                        "colTotals": False, "transposePivot": False, "combineMetric": False}), 12, 90)],
             [(raw_table("Latest transactions (streamed refresh rows appear here)", "bi_recent_sales",
                         ["sales_timestamp_utc", "generic_name", "channel_group", "census_region", "quantity_sold_packs",
                          "net_sales_usd", "ingest_mode", "refresh_tag"], "sales_timestamp_utc", limit=12), 12, 55)],
             [(raw_table("Refresh log (every BI publish)", "bi_refresh_history",
                         ["refresh_tag", "published_at", "data_as_of", "latest_demand_month", "fact_sales_rows",
                          "fact_hospital_rows", "medicines", "rows_published", "publish_seconds"], "published_at", limit=20), 12, 45)],
         ]),
]


# =============================================================================================== query context
def _filters_to_query(adhoc):
    out = []
    for f in adhoc:
        op = {"==": "==", "!=": "!=", "<=": "<=", ">=": ">=", "<": "<", ">": ">", "IN": "IN"}.get(f["operator"], f["operator"])
        out.append({"col": f["subject"], "op": op, "val": f["comparator"]})
    return out


def build_query_context(ds_id, viz, fd, temporal_x):
    q = {"filters": _filters_to_query(fd.get("adhoc_filters", [])), "extras": {"having": "", "where": ""},
         "applied_time_extras": {}, "time_range": "No filter", "row_limit": fd.get("row_limit", 10000),
         "series_limit": 0, "order_desc": True, "url_params": {}, "custom_params": {}, "custom_form_data": {}}
    metrics, columns, orderby = [], [], []
    if viz == "big_number_total":
        metrics = [fd["metric"]]
    elif viz in ("echarts_timeseries_line", "echarts_timeseries_bar", "echarts_area"):
        metrics = fd["metrics"]
        x = fd["x_axis"]
        base = ({"timeGrain": "P1M", "columnType": "BASE_AXIS", "sqlExpression": x, "label": x, "expressionType": "SQL"}
                if temporal_x else x)
        columns = [base] + list(fd.get("groupby", []))
        if temporal_x:
            q["extras"]["time_grain_sqla"] = "P1M"
        orderby = [[metrics[0], False]]
    elif viz in ("pie", "funnel", "treemap_v2"):
        metrics, columns = [fd["metric"]], list(fd["groupby"])
        orderby = [[fd["metric"], False]]
    elif viz == "sunburst_v2":
        metrics, columns = [fd["metric"]], list(fd["columns"])
    elif viz == "heatmap_v2":
        metrics, columns = [fd["metric"]], [fd["x_axis"], fd["groupby"]]
    elif viz == "bubble_v2":
        metrics, columns = [fd["x"], fd["y"], fd["size"]], [fd["entity"], fd["series"]]
    elif viz == "pivot_table_v2":
        metrics, columns = fd["metrics"], fd["groupbyRows"] + fd["groupbyColumns"]
    elif viz == "table" and fd.get("query_mode") == "raw":
        columns = fd["all_columns"]
        orderby = [json.loads(o) for o in fd.get("order_by_cols", [])]
    elif viz == "table":
        metrics, columns = fd["metrics"], fd["groupby"]
        if fd.get("timeseries_limit_metric"):
            orderby = [[fd["timeseries_limit_metric"], False]]
    q.update({"metrics": metrics, "columns": columns, "orderby": orderby})
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [q], "form_data": fd,
            "result_format": "json", "result_type": "full"}


# =============================================================================================== provisioning
def _ensure_database(api):
    payload = {"database_name": BI_DB_NAME, "sqlalchemy_uri": BI_DB_URI, "expose_in_sqllab": True,
               "allow_run_async": False, "allow_ctas": False, "allow_cvas": False, "allow_dml": False,
               "extra": json.dumps({"metadata_cache_timeout": {}, "schemas_allowed_for_file_upload": []})}
    db_id = api.find("database", "database_name", BI_DB_NAME)
    if db_id:
        api.call("PUT", f"database/{db_id}", json=payload)
    else:
        db_id = api.call("POST", "database/", json=payload)["id"]
    return db_id


def _ensure_dataset(api, db_id, table, spec):
    ds_id = None
    res = api.call("GET", "dataset/", params={"q": f"(filters:!((col:table_name,opr:eq,value:{table})),page_size:100)"})
    for r in res.get("result", []):
        if r.get("schema") == SCHEMA and r["table_name"] == table:
            ds_id = r["id"]
    if not ds_id:
        ds_id = api.call("POST", "dataset/", json={"database": db_id, "schema": SCHEMA, "table_name": table})["id"]
    api.call("PUT", f"dataset/{ds_id}/refresh")
    cur = api.call("GET", f"dataset/{ds_id}")["result"]
    existing = {m["metric_name"]: m["id"] for m in cur.get("metrics", [])}
    metrics = []
    for m in spec["metrics"]:
        d = dict(m)
        if m["metric_name"] in existing:
            d["id"] = existing[m["metric_name"]]
        metrics.append(d)
    if "count" in existing:
        metrics.append({"id": existing["count"], "metric_name": "count", "expression": "COUNT(*)", "verbose_name": "Rows"})
    body = {"description": spec["desc"], "metrics": metrics}
    if spec["dttm"]:
        body["main_dttm_col"] = spec["dttm"]
    api.call("PUT", f"dataset/{ds_id}", json=body)
    cols = {c["column_name"] for c in api.call("GET", f"dataset/{ds_id}")["result"]["columns"]}
    return ds_id, cols


def provision(api=None):
    api = api or Superset()
    db_id = _ensure_database(api)
    ds_ids, ds_cols = {}, {}
    for table, spec in DATASETS.items():
        ds_ids[table], ds_cols[table] = _ensure_dataset(api, db_id, table, spec)
    # dashboards first (charts reference them)
    dash_ids = {}
    for d in DASHBOARDS:
        did = api.find("dashboard", "slug", d["slug"])
        if not did:
            did = api.call("POST", "dashboard/", json={"dashboard_title": d["title"], "slug": d["slug"], "published": True})["id"]
        dash_ids[d["slug"]] = did
    # charts (shared charts such as the refresh banner live on several dashboards)
    chart_dashboards = {}
    for d in DASHBOARDS:
        for row in d["rows"]:
            for item, *_ in row:
                if item != "MARKDOWN":
                    chart_dashboards.setdefault(item["name"], set()).add(dash_ids[d["slug"]])
    chart_ids, chart_ds = {}, {}
    for d in DASHBOARDS:
        for row in d["rows"]:
            for item, *_ in row:
                if item == "MARKDOWN" or item["name"] in chart_ids:
                    continue
                ds = ds_ids[item["ds"]]
                fd = dict(COMMON)
                if item["viz"] == "heatmap_v2":
                    fd.pop("legendType", None)
                fd.update(item["fd"])
                fd.update({"datasource": f"{ds}__table", "viz_type": item["viz"]})
                temporal = fd.get("x_axis") == "month_date"
                qc = build_query_context(ds, item["viz"], fd, temporal)
                body = {"slice_name": item["name"], "viz_type": item["viz"], "datasource_id": ds,
                        "datasource_type": "table", "params": json.dumps(fd), "query_context": json.dumps(qc),
                        "dashboards": sorted(chart_dashboards[item["name"]]),
                        "description": f"SDI BI chart on sdi_bi.{item['ds']}"}
                cid = api.find("chart", "slice_name", item["name"])
                if cid:
                    api.call("PUT", f"chart/{cid}", json=body)
                else:
                    cid = api.call("POST", "chart/", json=body)["id"]
                chart_ids[item["name"]], chart_ds[item["name"]] = cid, item["ds"]
    CHART_ID_CACHE.update(chart_ids)
    # layouts + native filters
    for d in DASHBOARDS:
        pos, in_dash = _layout(d, chart_ids)
        meta = _metadata(d, in_dash, chart_ds, ds_ids, ds_cols)
        meta["positions"] = pos
        api.call("PUT", f"dashboard/{dash_ids[d['slug']]}",
                 json={"dashboard_title": d["title"], "slug": d["slug"], "published": True,
                       "position_json": json.dumps(pos), "json_metadata": json.dumps(meta),
                       "css": ".dashboard-markdown h3{margin-top:0} .header-title{font-weight:600}"})
    return {"database": db_id, "datasets": ds_ids, "dashboards": dash_ids, "charts": len(chart_ids)}


def _layout(d, chart_ids):
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": d["title"]}},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]}}
    in_dash = []
    for i, row in enumerate(d["rows"]):
        rid = f"ROW-{i}"
        pos["GRID_ID"]["children"].append(rid)
        pos[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for j, (item, width, height) in enumerate(row):
            if item == "MARKDOWN":
                cid = f"MARKDOWN-{i}-{j}"
                pos[cid] = {"type": "MARKDOWN", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                            "meta": {"width": width, "height": height, "code": d["story"]}}
            else:
                cid = f"CHART-{i}-{j}"
                pos[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                            "meta": {"width": width, "height": height, "chartId": chart_ids[item["name"]],
                                     "sliceName": item["name"], "uuid": None}}
                in_dash.append(item["name"])
            pos[rid]["children"].append(cid)
    return pos, in_dash


def _metadata(d, in_dash, chart_ds, ds_ids, ds_cols):
    filters = []
    all_ids = sorted({CHART_ID_CACHE.get(n) for n in in_dash} - {None})
    for name, col, src in d["filters"]:
        fid = f"NATIVE_FILTER-{d['slug']}-{col}"
        if col == "time":
            ok = [n for n in in_dash if DATASETS[chart_ds[n]]["dttm"] == "month_date"]
            f = {"id": fid, "name": name, "filterType": "filter_time", "targets": [{}], "controlValues": {},
                 "defaultDataMask": {"filterState": {"value": "No filter"}, "extraFormData": {"time_range": "No filter"},
                                     "ownState": {}}}
        else:
            ok = [n for n in in_dash if col in ds_cols[chart_ds[n]]]
            default = d.get("default_filters", {}).get(col)
            dm = ({"filterState": {"value": default, "label": ", ".join(default)},
                   "extraFormData": {"filters": [{"col": col, "op": "IN", "val": default}]}, "ownState": {}}
                  if default else {"filterState": {}, "extraFormData": {}, "ownState": {}})
            f = {"id": fid, "name": name, "filterType": "filter_select",
                 "targets": [{"datasetId": ds_ids[src], "column": {"name": col}}],
                 "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "defaultToFirstItem": False,
                                   "inverseSelection": False, "searchAllOptions": False},
                 "defaultDataMask": dm}
        in_scope = sorted({CHART_ID_CACHE[n] for n in ok})
        f.update({"type": "NATIVE_FILTER", "description": "", "cascadeParentIds": [], "tabsInScope": [],
                  "chartsInScope": in_scope,
                  "scope": {"rootPath": ["ROOT_ID"], "excluded": [c for c in all_ids if c not in in_scope]}})
        filters.append(f)
    return {"native_filter_configuration": filters, "refresh_frequency": REFRESH_SECONDS, "color_scheme": "supersetColors",
            "cross_filters_enabled": True, "timed_refresh_immune_slices": [], "expanded_slices": {},
            "label_colors": {"Retail pharmacy": "#1FA8C9", "Hospital pharmacy": "#454E7C", "E-commerce": "#5AC189",
                             "Hospital ward": "#FF7F44", "Y": "#E04355", "N": "#1FA8C9"},
            "shared_label_colors": {}, "default_filters": "{}", "filter_scopes": {}, "chart_configuration": {},
            "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": all_ids}}


CHART_ID_CACHE = {}


def provision_all():
    return provision(Superset())


def _chart_ids(api):
    out, page = {}, 0
    while True:
        r = api.call("GET", "chart/", params={"q": f"(page:{page},page_size:100)"})
        for c in r.get("result", []):
            out[c["slice_name"]] = c["id"]
        if len(r.get("result", [])) < 100:
            return out
        page += 1


# =============================================================================================== refresh & warm-up
def refresh_datasets():
    api = Superset()
    out = {}
    for table in DATASETS:
        res = api.call("GET", "dataset/", params={"q": f"(filters:!((col:table_name,opr:eq,value:{table})),page_size:10)"})
        for r in res.get("result", []):
            if r.get("schema") == SCHEMA:
                api.call("PUT", f"dataset/{r['id']}/refresh")
                out[table] = r["id"]
    return out


def warm(run_id=None, refresh_tag=None, log=True):
    """Fetches every dashboard chart through Superset (force=true, i.e. live query), timed.
    Each fetch is one 'BI component fetching data' -> ops.query_log (caller='bi_dashboard', tier='bi_superset')."""
    api = Superset()
    rows, summary = [], {}
    for d in DASHBOARDS:
        did = api.find("dashboard", "slug", d["slug"])
        charts = api.call("GET", f"dashboard/{did}/charts")["result"]
        for c in charts:
            cid, name = c["id"], c["slice_name"]
            started = dt.datetime.now(dt.timezone.utc)
            t0 = time.perf_counter()
            try:
                r = api.s.get(f"{api.url}/api/v1/chart/{cid}/data/", params={"force": "true", "format": "json"}, timeout=120)
                ms = (time.perf_counter() - t0) * 1000
                body = r.json()
                ok = r.status_code == 200 and all(not x.get("error") for x in body.get("result", []))
                nrows = sum(int(x.get("rowcount") or 0) for x in body.get("result", []))
                err = None if ok else (json.dumps(body)[:500])
            except Exception as e:
                ms, ok, nrows, err = (time.perf_counter() - t0) * 1000, False, 0, str(e)[:500]
            rows.append((started, run_id, refresh_tag, "bi_dashboard", f"{d['slug'].split('-')[2].upper()}#{cid}", name,
                         d["title"], "bi_superset", "postgres", round(ms, 3), nrows, "SUCCESS" if ok else "FAILED", err))
            summary[f"{d['slug']}:{name}"] = (round(ms, 1), nrows, ok)
    if log:
        from ..db import pg_conn
        import psycopg2.extras
        with pg_conn() as c, c.cursor() as cur:
            psycopg2.extras.execute_values(cur, """INSERT INTO ops.query_log (executed_at, run_id, refresh_tag, caller,
                query_id, widget_name, business_question, tier, engine, exec_ms, rows_returned, status, error_message)
                VALUES %s""", rows)
    failed = [k for k, v in summary.items() if not v[2]]
    return {"charts": len(summary), "failed": failed, "avg_ms": round(sum(v[0] for v in summary.values()) / max(len(summary), 1), 1)}


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "provision"
    if cmd == "provision":
        print(json.dumps(provision_all(), indent=1, default=str))
    elif cmd == "warm":
        print(json.dumps(warm(log="--no-log" not in sys.argv), indent=1))
    elif cmd == "refresh":
        print(refresh_datasets())
