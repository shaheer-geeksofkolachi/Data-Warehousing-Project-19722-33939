"""BI serving layer: publishes the DuckDB SDI mart (gold) to the Postgres schema `sdi_bi`, which Apache Superset reads.

Why a serving layer: DuckDB allows one writer and blocks readers from other processes while Airflow writes, so a BI
tool reading the .duckdb file directly would collide with the pipeline. Instead, after every transform:

  1. BI tables are computed in DuckDB (denormalised, BI-friendly: one dataset per dashboard theme)
  2. they are copied, together with the star schema (dims + facts), into a NEW schema `sdi_bi_new`
  3. `sdi_bi_new` is swapped in for `sdi_bi` in one transaction -> dashboards never see a half-loaded state
  4. a least-privilege role `bi_reader` (used by Superset) gets SELECT on the new schema
  5. the refresh is recorded in ops.bi_refresh_log and the headline KPIs in ops.bi_kpi_snapshot

Medallion view of the platform: bronze = landing Excel / stream events, silver = Postgres staging, gold = DuckDB mart
(+ Parquet), serving = Postgres sdi_bi (BI).
"""
import os
import time
import datetime as dt
from pathlib import Path

import duckdb

from ..config import DUCKDB_PATH, WORK_DIR
from ..db import pg_conn, pg_query, mart_lock

BI_SCHEMA = "sdi_bi"
BI_READER = os.getenv("SDI_BI_READER_USER", "bi_reader")
BI_READER_PASSWORD = os.getenv("SDI_BI_READER_PASSWORD", "bi_reader")

# ------------------------------------------------------------------------------------------------ BI tables (DuckDB SQL)
HOSPITAL_SQL = """
    SELECT d.month_start_date AS month_date, d.year_month, d.year_num, d.quarter_label,
           h.hospital_id, h.hospital_name, h.hospital_type, h.bed_size_band, h.licensed_beds, h.icu_beds,
           g.census_region, g.state_code, g.city,
           m.medicine_id, m.generic_name, m.therapeutic_class, m.disease_category, m.cold_chain_flag,
           p.medication_request_status, p.formulary_status, f.refresh_tag,
           f.forecast_demand_packs AS forecast_packs, f.consumed_qty_packs AS consumed_packs,
           f.requested_qty_packs AS requested_packs, f.approved_qty_packs AS approved_packs,
           f.received_qty_packs AS received_packs, f.dispensed_qty_packs AS dispensed_packs,
           f.emergency_request_packs AS emergency_packs, f.unfulfilled_demand_packs AS unfulfilled_packs,
           f.stockout_days, f.wastage_qty_packs AS wastage_packs, f.opening_stock_packs AS opening_stock_packs,
           f.closing_stock_packs AS closing_stock_packs, f.forecast_error_packs, f.abs_forecast_error_packs AS abs_error_packs,
           f.supply_gap_packs, f.service_level_pct
    FROM fact_hospital_demand f
    JOIN dim_date d ON d.date_key = f.demand_month_key
    JOIN dim_hospital h ON h.hospital_key = f.hospital_key
    JOIN dim_medicine m ON m.medicine_key = f.medicine_key
    JOIN dim_geography g ON g.geography_key = f.geography_key
    JOIN dim_demand_profile p ON p.demand_profile_key = f.demand_profile_key"""

SALES_SQL = """
    SELECT d.month_start_date AS month_date, d.year_month, d.year_num, CAST(d.year_num AS VARCHAR) AS year_label,
           m.medicine_id, m.generic_name, m.brand_name, m.therapeutic_class, m.disease_category, m.cold_chain_flag,
           m.price_band, c.channel_group, g.census_region, g.state_code, pr.promotion_type, sp.customer_segment,
           sp.date_quality_flag, f.ingest_mode, f.refresh_tag,
           count(*) AS txn_count, sum(f.quantity_sold_packs) AS packs, sum(f.gross_sales_usd) AS gross_usd,
           sum(f.discount_usd) AS discount_usd, sum(f.net_sales_usd) AS net_usd, sum(f.cogs_usd) AS cogs_usd,
           sum(f.gross_margin_usd) AS margin_usd, sum(f.returned_qty_packs) AS returned_packs,
           count(*) FILTER (WHERE sp.ecommerce_delivery_status = 'Returned to sender') AS rts_txn,
           count(*) FILTER (WHERE c.channel_group = 'E-commerce') AS ecom_orders,
           count(*) FILTER (WHERE pr.is_promoted) AS promoted_txn,
           sum(f.days_to_expiry_at_sale) AS days_to_expiry_sum
    FROM fact_sales f
    JOIN dim_date d ON d.date_key = f.sale_date_key
    JOIN dim_medicine m ON m.medicine_key = f.medicine_key
    JOIN dim_channel c ON c.channel_key = f.channel_key
    JOIN dim_geography g ON g.geography_key = f.geography_key
    JOIN dim_promotion pr ON pr.promotion_key = f.promotion_key
    JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
    GROUP BY ALL"""

BI_TABLES = {
    # BI-1 Hospital forecast vs actual & unfulfilled demand (grain: hospital x medicine x month)
    "bi_hospital_demand": HOSPITAL_SQL,
    # BI-1 funnel: long format request -> approval -> receipt -> consumption
    "bi_supply_funnel": f"""
        WITH h AS ({HOSPITAL_SQL})
        SELECT month_date, year_month, hospital_name, census_region, disease_category, therapeutic_class,
               stage_order, stage, packs
        FROM h, LATERAL (VALUES (1, '1 Requested', requested_packs), (2, '2 Approved', approved_packs),
                                (3, '3 Received', received_packs), (4, '4 Consumed', consumed_packs))
             AS s(stage_order, stage, packs)""",
    # BI-2 / BI-3 sales cube (grain: month x medicine x channel x state x promo type x segment x flags)
    "bi_sales_monthly": SALES_SQL,
    # BI-3 medicine velocity (normal postings only)
    "bi_medicine_velocity": f"""
        WITH s AS ({SALES_SQL}),
        v AS (SELECT medicine_id, any_value(generic_name) AS generic_name, any_value(brand_name) AS brand_name,
                     any_value(therapeutic_class) AS therapeutic_class, any_value(disease_category) AS disease_category,
                     any_value(cold_chain_flag) AS cold_chain_flag,
                     sum(packs) AS packs, sum(net_usd) AS net_usd, sum(margin_usd) AS margin_usd,
                     sum(txn_count) AS txn_count, sum(days_to_expiry_sum) / sum(txn_count) AS avg_days_to_expiry,
                     count(DISTINCT year_month) AS active_months, min(year_month) AS first_month,
                     max(year_month) AS last_month
              FROM s WHERE date_quality_flag = 'Normal' GROUP BY 1)
        , r AS (SELECT v.*, packs / active_months AS packs_per_month, net_usd / active_months AS net_per_month,
               rank() OVER (PARTITION BY therapeutic_class ORDER BY packs / active_months DESC) AS rank_in_class,
               rank() OVER (ORDER BY packs / active_months DESC) AS overall_rank,
               rank() OVER (ORDER BY packs / active_months ASC) AS slow_rank,
               CASE ntile(3) OVER (ORDER BY packs / active_months DESC) WHEN 1 THEN 'Fast' WHEN 2 THEN 'Medium'
                    ELSE 'Slow' END AS velocity_class,
               CASE WHEN first_month >= '2026-01' THEN 'New launch' ELSE 'Established' END AS lifecycle
               FROM v)
        SELECT r.*, m.recommended_retail_price_usd AS current_rrp_usd
        FROM r JOIN dim_medicine m ON m.medicine_id = r.medicine_id AND m.is_current""",
    # BI-4 unified demand (sales channels + hospital wards on conformed dimensions)
    "bi_unified_demand": """
        SELECT strptime(year_month || '-01', '%Y-%m-%d')::DATE AS month_date, *
        FROM agg_unified_demand_monthly""",
    # BI-4 demand vs supply per month x region x disease
    "bi_demand_supply_monthly": f"""
        WITH s AS (SELECT month_date, census_region, disease_category, sum(packs) AS commercial_packs,
                          sum(net_usd) AS net_usd
                   FROM ({SALES_SQL}) WHERE date_quality_flag = 'Normal' GROUP BY ALL),
             h AS (SELECT month_date, census_region, disease_category, sum(consumed_packs) AS ward_consumed_packs,
                          sum(forecast_packs) AS forecast_packs, sum(requested_packs) AS requested_packs,
                          sum(received_packs) AS received_packs, sum(unfulfilled_packs) AS unfulfilled_packs,
                          sum(emergency_packs) AS emergency_packs
                   FROM ({HOSPITAL_SQL}) GROUP BY ALL)
        SELECT coalesce(s.month_date, h.month_date) AS month_date,
               strftime(coalesce(s.month_date, h.month_date), '%Y-%m') AS year_month,
               coalesce(s.census_region, h.census_region) AS census_region,
               coalesce(s.disease_category, h.disease_category) AS disease_category,
               coalesce(commercial_packs, 0) AS commercial_packs, coalesce(net_usd, 0) AS net_usd,
               coalesce(ward_consumed_packs, 0) AS ward_consumed_packs, coalesce(forecast_packs, 0) AS forecast_packs,
               coalesce(requested_packs, 0) AS requested_packs, coalesce(received_packs, 0) AS received_packs,
               coalesce(unfulfilled_packs, 0) AS unfulfilled_packs, coalesce(emergency_packs, 0) AS emergency_packs
        FROM s FULL OUTER JOIN h USING (month_date, census_region, disease_category)""",
    # BI-4 live feed: newest transactions (shows streamed refresh rows arriving)
    "bi_recent_sales": """
        SELECT f.sales_timestamp_utc, d.full_date AS sale_date, f.sales_transaction_id, m.generic_name,
               p.pharmacy_name, c.channel_group, g.census_region, f.quantity_sold_packs, f.net_sales_usd,
               f.ingest_mode, f.refresh_tag
        FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key
        JOIN dim_medicine m ON m.medicine_key = f.medicine_key JOIN dim_pharmacy p ON p.pharmacy_key = f.pharmacy_key
        JOIN dim_channel c ON c.channel_key = f.channel_key JOIN dim_geography g ON g.geography_key = f.geography_key
        ORDER BY f.sales_timestamp_utc DESC LIMIT 1000""",
}
# the gold star schema is replicated as-is (drill-to-detail, ad-hoc SQL Lab)
STAR_TABLES = ["dim_date", "dim_medicine", "dim_hospital", "dim_pharmacy", "dim_geography", "dim_warehouse",
               "dim_channel", "dim_promotion", "dim_sales_profile", "dim_demand_profile", "fact_sales",
               "fact_hospital_demand"]
INDEXES = {"bi_hospital_demand": ["month_date", "hospital_name", "disease_category"],
           "bi_supply_funnel": ["month_date"], "bi_sales_monthly": ["month_date", "channel_group", "medicine_id"],
           "bi_unified_demand": ["month_date"], "bi_demand_supply_monthly": ["month_date"],
           "fact_sales": ["sale_date_key", "medicine_key"], "fact_hospital_demand": ["demand_month_key"]}

PG_TYPE = {"VARCHAR": "TEXT", "INTEGER": "INTEGER", "BIGINT": "BIGINT", "SMALLINT": "SMALLINT", "TINYINT": "SMALLINT",
           "HUGEINT": "NUMERIC", "DOUBLE": "DOUBLE PRECISION", "FLOAT": "REAL", "DATE": "DATE", "BOOLEAN": "BOOLEAN",
           "TIMESTAMP": "TIMESTAMP", "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ", "UINTEGER": "BIGINT", "UBIGINT": "NUMERIC"}

OPS_DDL = """
CREATE TABLE IF NOT EXISTS ops.bi_refresh_log (
    refresh_id        BIGSERIAL PRIMARY KEY,
    published_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id            TEXT,
    refresh_tag       TEXT,          -- latest data refresh contained in the mart (BASE / R1 / R2)
    data_as_of        DATE,          -- latest sale date in the mart
    latest_demand_month DATE,
    fact_sales_rows   BIGINT,
    fact_hospital_rows BIGINT,
    medicines         INT,
    tables_published  INT,
    rows_published    BIGINT,
    publish_seconds   NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS ops.bi_publish_log (
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id        TEXT,
    table_name    TEXT,
    row_count     BIGINT,
    seconds       NUMERIC(10,3)
);
CREATE TABLE IF NOT EXISTS ops.bi_kpi_snapshot (
    snapshot_id   BIGSERIAL PRIMARY KEY,
    snapshot_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_id        TEXT,
    refresh_tag   TEXT,
    dashboard     TEXT,
    kpi_order     INT,
    kpi_name      TEXT,
    kpi_value     DOUBLE PRECISION,
    unit          TEXT
);
"""

# Headline KPIs (computed on the published serving layer after every refresh -> before/after evidence)
KPIS = [
    ("BI-1 Hospital", 1, "Forecast bias %", "%",
     "SELECT 100.0*sum(forecast_error_packs)/nullif(sum(consumed_packs),0) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 2, "Forecast error (WAPE) %", "%",
     "SELECT 100.0*sum(abs_error_packs)/nullif(sum(consumed_packs),0) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 3, "Request-to-receipt fill rate %", "%",
     "SELECT 100.0*sum(received_packs)/nullif(sum(requested_packs),0) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 4, "Supply gap (packs)", "packs",
     "SELECT sum(requested_packs - received_packs) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 5, "Unfulfilled demand (packs)", "packs", "SELECT sum(unfulfilled_packs) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 6, "Emergency demand %", "%",
     "SELECT 100.0*sum(emergency_packs)/nullif(sum(consumed_packs),0) FROM {s}.bi_hospital_demand"),
    ("BI-1 Hospital", 7, "Latest month forecast bias %", "%",
     "SELECT 100.0*sum(forecast_error_packs)/nullif(sum(consumed_packs),0) FROM {s}.bi_hospital_demand "
     "WHERE month_date = (SELECT max(month_date) FROM {s}.bi_hospital_demand)"),
    ("BI-2 Channel", 10, "Net revenue USD (normal postings)", "USD",
     "SELECT sum(net_usd) FROM {s}.bi_sales_monthly WHERE date_quality_flag='Normal'"),
    ("BI-2 Channel", 11, "Gross margin %", "%",
     "SELECT 100.0*sum(margin_usd)/nullif(sum(net_usd),0) FROM {s}.bi_sales_monthly WHERE date_quality_flag='Normal'"),
    ("BI-2 Channel", 12, "E-commerce share of revenue %", "%",
     "SELECT 100.0*sum(net_usd) FILTER (WHERE channel_group='E-commerce')/nullif(sum(net_usd),0) "
     "FROM {s}.bi_sales_monthly WHERE date_quality_flag='Normal'"),
    ("BI-2 Channel", 13, "Hospital-pharmacy share of revenue %", "%",
     "SELECT 100.0*sum(net_usd) FILTER (WHERE channel_group='Hospital pharmacy')/nullif(sum(net_usd),0) "
     "FROM {s}.bi_sales_monthly WHERE date_quality_flag='Normal'"),
    ("BI-2 Channel", 14, "Latest month net revenue USD", "USD",
     "SELECT sum(net_usd) FROM {s}.bi_sales_monthly WHERE date_quality_flag='Normal' "
     "AND month_date = (SELECT max(month_date) FROM {s}.bi_sales_monthly)"),
    ("BI-2 Channel", 15, "E-commerce returned-to-sender %", "%",
     "SELECT 100.0*sum(rts_txn)/nullif(sum(ecom_orders),0) FROM {s}.bi_sales_monthly"),
    ("BI-3 Velocity", 20, "Medicines on sale", "count", "SELECT count(*) FROM {s}.bi_medicine_velocity"),
    ("BI-3 Velocity", 21, "Cold-chain share of revenue %", "%",
     "SELECT 100.0*sum(net_usd) FILTER (WHERE cold_chain_flag='Y')/nullif(sum(net_usd),0) FROM {s}.bi_medicine_velocity"),
    ("BI-3 Velocity", 22, "Cold-chain share of packs %", "%",
     "SELECT 100.0*sum(packs) FILTER (WHERE cold_chain_flag='Y')/nullif(sum(packs),0) FROM {s}.bi_medicine_velocity"),
    ("BI-3 Velocity", 23, "Fastest mover packs / month", "packs",
     "SELECT max(packs_per_month) FROM {s}.bi_medicine_velocity"),
    ("BI-4 Unified", 30, "Unified demand packs (all channels incl. wards)", "packs",
     "SELECT sum(demand_packs) FROM {s}.bi_unified_demand"),
    ("BI-4 Unified", 31, "Hospital-ward share of unified demand %", "%",
     "SELECT 100.0*sum(demand_packs) FILTER (WHERE channel_group='Hospital ward')/nullif(sum(demand_packs),0) "
     "FROM {s}.bi_unified_demand"),
    ("BI-4 Unified", 32, "Sales transactions in mart", "rows", "SELECT count(*) FROM {s}.fact_sales"),
    ("BI-4 Unified", 33, "Months of history", "months", "SELECT count(DISTINCT year_month) FROM {s}.bi_unified_demand"),
]


def _pg_columns(con, sql):
    return [(name, PG_TYPE.get(str(typ).split("(")[0].upper(), "NUMERIC" if str(typ).upper().startswith("DECIMAL")
                               else "TEXT"))
            for name, typ, *_ in con.execute(f"DESCRIBE ({sql})").fetchall()]


def ensure_ops():
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(OPS_DDL)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (BI_READER,))
        if not cur.fetchone():
            cur.execute(f"CREATE ROLE {BI_READER} LOGIN PASSWORD %s", (BI_READER_PASSWORD,))


def publish(run_id: str = None, refresh_tag: str = None) -> dict:
    run_id = run_id or f"BI-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}"
    ensure_ops()
    t_all = time.perf_counter()
    stage = Path(WORK_DIR) / f"bi_{run_id.replace(':', '_').replace('+', '_')}"
    stage.mkdir(parents=True, exist_ok=True)
    plan = [(t, f"SELECT * FROM {t}") for t in STAR_TABLES] + list(BI_TABLES.items())
    specs = {}
    with mart_lock():                                       # read a consistent mart, then release for Airflow
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        for name, sql in plan:
            t0 = time.perf_counter()
            specs[name] = _pg_columns(con, sql)
            f = stage / f"{name}.csv"
            con.execute(f"COPY ({sql}) TO '{f}' (HEADER, DELIMITER ',', NULLSTR '\\N')")
            specs[name] = (specs[name], f, time.perf_counter() - t0)
        tag = refresh_tag or con.execute(
            "SELECT refresh_tag FROM fact_sales WHERE refresh_tag IS NOT NULL ORDER BY etl_loaded_at DESC LIMIT 1").fetchone()[0]
        as_of = con.execute("SELECT max(d.full_date) FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key").fetchone()[0]
        dem = con.execute("SELECT max(d.full_date) FROM fact_hospital_demand f JOIN dim_date d "
                          "ON d.date_key = f.demand_month_key").fetchone()[0]
        n_sales, n_hosp = (con.execute("SELECT count(*) FROM fact_sales").fetchone()[0],
                           con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0])
        n_med = con.execute("SELECT count(DISTINCT medicine_id) FROM dim_medicine WHERE medicine_key > 0").fetchone()[0]
        con.close()
    new = f"{BI_SCHEMA}_new"
    total_rows = 0
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {new} CASCADE; CREATE SCHEMA {new}")
        for name, (cols, f, extract_s) in specs.items():
            t0 = time.perf_counter()
            cur.execute(f"CREATE TABLE {new}.{name} ({', '.join(f'{n} {t}' for n, t in cols)})")
            with open(f) as fh:
                cur.copy_expert(f"COPY {new}.{name} FROM STDIN WITH (FORMAT csv, HEADER, NULL '\\N')", fh)
            cur.execute(f"SELECT count(*) FROM {new}.{name}")
            n = cur.fetchone()[0]
            total_rows += n
            for col in INDEXES.get(name, []):
                cur.execute(f"CREATE INDEX ON {new}.{name} ({col})")
            cur.execute("INSERT INTO ops.bi_publish_log (run_id, table_name, row_count, seconds) VALUES (%s,%s,%s,%s)",
                        (run_id, name, n, round(extract_s + time.perf_counter() - t0, 3)))
            os.remove(f)
        # views over ops (they live in the serving schema so Superset sees one schema)
        cur.execute(f"""CREATE VIEW {new}.bi_refresh_history AS
                        SELECT refresh_id, published_at, refresh_tag, data_as_of, latest_demand_month, fact_sales_rows,
                               fact_hospital_rows, medicines, rows_published, publish_seconds
                        FROM ops.bi_refresh_log""")
        cur.execute(f"""CREATE VIEW {new}.bi_kpi_history AS
                        SELECT DISTINCT ON (refresh_tag, kpi_name) refresh_tag, dashboard, kpi_order, kpi_name, kpi_value,
                               unit, snapshot_ts
                        FROM ops.bi_kpi_snapshot ORDER BY refresh_tag, kpi_name, snapshot_ts DESC""")
        cur.execute(f"ANALYZE")
        # atomic swap: Superset keeps querying sdi_bi by name and never sees a half-built schema
        cur.execute(f"DROP SCHEMA IF EXISTS {BI_SCHEMA}_old CASCADE")
        cur.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (BI_SCHEMA,))
        if cur.fetchone():
            cur.execute(f"ALTER SCHEMA {BI_SCHEMA} RENAME TO {BI_SCHEMA}_old")
        cur.execute(f"ALTER SCHEMA {new} RENAME TO {BI_SCHEMA}")
        cur.execute(f"DROP SCHEMA IF EXISTS {BI_SCHEMA}_old CASCADE")
        # least privilege: the BI tool reads the serving schema (and the two ops tables behind its views) only
        cur.execute(f"GRANT USAGE ON SCHEMA {BI_SCHEMA} TO {BI_READER}")
        cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {BI_SCHEMA} TO {BI_READER}")
        cur.execute(f"GRANT USAGE ON SCHEMA ops TO {BI_READER}")
        cur.execute(f"GRANT SELECT ON ops.bi_refresh_log, ops.bi_kpi_snapshot TO {BI_READER}")
        secs = round(time.perf_counter() - t_all, 2)
        cur.execute("""INSERT INTO ops.bi_refresh_log (run_id, refresh_tag, data_as_of, latest_demand_month, fact_sales_rows,
                       fact_hospital_rows, medicines, tables_published, rows_published, publish_seconds)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, tag, as_of, dem, n_sales, n_hosp, n_med, len(specs), total_rows, secs))
    try:
        stage.rmdir()
    except OSError:
        pass
    return {"run_id": run_id, "refresh_tag": tag, "data_as_of": str(as_of), "tables": len(specs),
            "rows": total_rows, "seconds": secs}


def snapshot_kpis(run_id: str = None, refresh_tag: str = None) -> dict:
    """Records the headline KPIs of every dashboard for this refresh (ops.bi_kpi_snapshot)."""
    ensure_ops()
    if refresh_tag is None:
        refresh_tag = pg_query("SELECT refresh_tag FROM ops.bi_refresh_log ORDER BY refresh_id DESC LIMIT 1")[0][0]
    out = {}
    with pg_conn() as c, c.cursor() as cur:
        for dash, order, name, unit, sql in KPIS:
            cur.execute(sql.format(s=BI_SCHEMA))
            v = cur.fetchone()[0]
            v = float(v) if v is not None else None
            out[name] = v
            cur.execute("""INSERT INTO ops.bi_kpi_snapshot (run_id, refresh_tag, dashboard, kpi_order, kpi_name,
                           kpi_value, unit) VALUES (%s,%s,%s,%s,%s,%s,%s)""", (run_id, refresh_tag, dash, order, name, v, unit))
    return out


if __name__ == "__main__":
    print(publish())
    print(snapshot_kpis())
