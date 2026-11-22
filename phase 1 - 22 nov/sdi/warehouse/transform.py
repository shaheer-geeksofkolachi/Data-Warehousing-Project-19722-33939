"""Transformation & dimensional modelling: Postgres staging  ->  DuckDB SDI star schema.

Each public function is one Airflow task (sdi_03_transform DAG):
    init_mart -> extract_delta -> load_dimensions -> load_fact_sales -> load_fact_hospital_demand
              -> build_aggregates -> reconcile_and_commit

Incremental: only staging rows with _stg_id above the last committed watermark are processed. Watermarks are
committed ONLY after reconciliation passes, so a failed run is simply re-run (fact loads are anti-joined on the
degenerate transaction id -> idempotent).
"""
import io
import json
import time
import datetime as dt
from pathlib import Path

import pandas as pd

from ..config import PROJECT_ROOT, WORK_DIR
from ..db import pg_conn, pg_query, duck_conn, mart_lock, set_mart_state
from ..sources import SOURCES, snake

DUCK_TYPES = {"text": "VARCHAR", "bigint": "BIGINT", "double": "DOUBLE", "date": "DATE", "timestamp": "TIMESTAMPTZ"}
MART_SOURCES = ["01_master_data", "03_manufacturer_delivery_data", "05_pharmacy_sales_data", "06_hospital_demand_data"]
BULK_POSTING_GAP_DAYS = 240

STATES = {  # code: (name, census region, census division)
    "AL": ("Alabama", "South", "East South Central"), "AK": ("Alaska", "West", "Pacific"),
    "AZ": ("Arizona", "West", "Mountain"), "AR": ("Arkansas", "South", "West South Central"),
    "CA": ("California", "West", "Pacific"), "CO": ("Colorado", "West", "Mountain"),
    "CT": ("Connecticut", "Northeast", "New England"), "DE": ("Delaware", "South", "South Atlantic"),
    "DC": ("District of Columbia", "South", "South Atlantic"), "FL": ("Florida", "South", "South Atlantic"),
    "GA": ("Georgia", "South", "South Atlantic"), "HI": ("Hawaii", "West", "Pacific"),
    "ID": ("Idaho", "West", "Mountain"), "IL": ("Illinois", "Midwest", "East North Central"),
    "IN": ("Indiana", "Midwest", "East North Central"), "IA": ("Iowa", "Midwest", "West North Central"),
    "KS": ("Kansas", "Midwest", "West North Central"), "KY": ("Kentucky", "South", "East South Central"),
    "LA": ("Louisiana", "South", "West South Central"), "ME": ("Maine", "Northeast", "New England"),
    "MD": ("Maryland", "South", "South Atlantic"), "MA": ("Massachusetts", "Northeast", "New England"),
    "MI": ("Michigan", "Midwest", "East North Central"), "MN": ("Minnesota", "Midwest", "West North Central"),
    "MS": ("Mississippi", "South", "East South Central"), "MO": ("Missouri", "Midwest", "West North Central"),
    "MT": ("Montana", "West", "Mountain"), "NE": ("Nebraska", "Midwest", "West North Central"),
    "NV": ("Nevada", "West", "Mountain"), "NH": ("New Hampshire", "Northeast", "New England"),
    "NJ": ("New Jersey", "Northeast", "Middle Atlantic"), "NM": ("New Mexico", "West", "Mountain"),
    "NY": ("New York", "Northeast", "Middle Atlantic"), "NC": ("North Carolina", "South", "South Atlantic"),
    "ND": ("North Dakota", "Midwest", "West North Central"), "OH": ("Ohio", "Midwest", "East North Central"),
    "OK": ("Oklahoma", "South", "West South Central"), "OR": ("Oregon", "West", "Pacific"),
    "PA": ("Pennsylvania", "Northeast", "Middle Atlantic"), "RI": ("Rhode Island", "Northeast", "New England"),
    "SC": ("South Carolina", "South", "South Atlantic"), "SD": ("South Dakota", "Midwest", "West North Central"),
    "TN": ("Tennessee", "South", "East South Central"), "TX": ("Texas", "South", "West South Central"),
    "UT": ("Utah", "West", "Mountain"), "VT": ("Vermont", "Northeast", "New England"),
    "VA": ("Virginia", "South", "South Atlantic"), "WA": ("Washington", "West", "Pacific"),
    "WV": ("West Virginia", "South", "South Atlantic"), "WI": ("Wisconsin", "Midwest", "East North Central"),
    "WY": ("Wyoming", "West", "Mountain")}

MEDICINE_T2 = ["pack_size", "prescription_class", "cold_chain_flag", "shelf_life_months", "unit_manufacturing_cost_usd",
               "wholesale_price_usd", "recommended_retail_price_usd", "product_status"]
MEDICINE_T1 = ["generic_name", "brand_name", "therapeutic_class", "atc_code", "strength", "internal_sku", "gtin14"]
HOSPITAL_T2 = ["hospital_type", "licensed_beds", "icu_beds", "avg_occupancy_rate_pct", "trauma_level",
               "catchment_population"]


# ------------------------------------------------------------------------------------------------ logging
def log_step(run_id, refresh_tag, step, target, rows_in=0, inserted=0, updated=0, t0=None, status="SUCCESS", details=""):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute("""INSERT INTO ops.transform_log (run_id,refresh_tag,step,target_table,rows_in,rows_inserted,
                       rows_updated,duration_s,status,details) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (run_id, refresh_tag, step, target, int(rows_in), int(inserted), int(updated),
                     round(time.perf_counter() - t0, 3) if t0 else None, status, details[:2000]))


def _manifest_path(run_id):
    d = WORK_DIR / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d / "manifest.json"


def _manifest(run_id):
    return json.loads(_manifest_path(run_id).read_text())


def _delta_view(con, name, manifest):
    """Registers the extracted staging delta CSV as a typed DuckDB view."""
    src = SOURCES[name]
    info = manifest["tables"][name]
    cols = {snake(c): DUCK_TYPES[t] for c, t in src.columns}
    cols.update({"_stg_id": "BIGINT", "_batch_id": "VARCHAR", "_refresh_tag": "VARCHAR", "_ingest_mode": "VARCHAR",
                 "_loaded_at": "TIMESTAMPTZ"})
    spec = ", ".join(f"'{k}': '{v}'" for k, v in cols.items())
    con.execute(f"CREATE OR REPLACE TEMP VIEW delta_{name[3:]} AS SELECT * FROM read_csv('{info['file']}', "
                f"header=true, columns={{{spec}}}, nullstr='\\N', quote='\"', escape='\"')")
    return f"delta_{name[3:]}"


def locked(fn):
    """Every task that opens the DuckDB mart runs under the cross-process mart lock (wait, never collide)."""
    import functools

    @functools.wraps(fn)
    def wrapper(*a, **kw):
        with mart_lock():
            return fn(*a, **kw)
    return wrapper


# ------------------------------------------------------------------------------------------------ tasks
@locked
def init_mart():
    con = duck_conn()
    con.execute((PROJECT_ROOT / "sql/duckdb/01_star_schema.sql").read_text())
    if con.execute("SELECT count(*) FROM dim_date").fetchone()[0] == 0:
        con.execute("""
            INSERT INTO dim_date
            SELECT CAST(strftime(d, '%Y%m%d') AS INT), d, day(d), isodow(d), dayname(d), isodow(d) >= 6,
                   weekofyear(d), month(d), monthname(d), strftime(d, '%b'), quarter(d),
                   CAST(year(d) AS VARCHAR) || '-Q' || CAST(quarter(d) AS VARCHAR), year(d), strftime(d, '%Y-%m'),
                   date_trunc('month', d)::DATE, day(d) = 1, (month(d) = 1 AND day(d) = 1),
                   day(last_day(d))
            FROM (SELECT unnest(generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY))::DATE AS d)""")
        con.execute("INSERT INTO dim_date VALUES (-1, DATE '1900-01-01', NULL, NULL, 'Unknown', NULL, NULL, NULL, "
                    "'Unknown', 'Unk', NULL, 'Unknown', NULL, 'Unknown', NULL, FALSE, FALSE, NULL)")
    unknowns = {
        "dim_geography": "(-1,'Unknown','UN','Unknown','Unknown','Unknown','UN')",
        "dim_medicine": "(-1,'UNKNOWN',NULL,NULL,'Unknown','Unknown','Unknown',NULL,NULL,'Unknown',NULL,NULL,NULL,NULL,"
                        "NULL,NULL,NULL,'Unknown','Unknown',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,"
                        "NULL,NULL,'Unknown','Unknown',NULL,DATE '1900-01-01',DATE '9999-12-31',TRUE,1,NULL)",
        "dim_hospital": "(-1,'UNKNOWN',NULL,'Unknown','Unknown',NULL,NULL,NULL,-1,NULL,NULL,NULL,NULL,NULL,'Unknown',"
                        "NULL,DATE '1900-01-01',DATE '9999-12-31',TRUE,1)",
        "dim_pharmacy": "(-1,'UNKNOWN',NULL,'Unknown','Unknown',NULL,NULL,NULL,-1,NULL)",
        "dim_warehouse": "(-1,'UNKNOWN',NULL,'Unknown',NULL,NULL,-1)",
        "dim_channel": "(-1,'Unknown','Unknown','Unknown','Unknown',NULL)",
        "dim_promotion": "(-1,'UNKNOWN','Unknown',NULL,FALSE)",
        "dim_sales_profile": "(-1,'Unknown','Unknown','Unknown',NULL,NULL,NULL,NULL,'Unknown')",
        "dim_demand_profile": "(-1,'Unknown','Unknown',NULL,NULL,NULL)",
    }
    for t, v in unknowns.items():
        key = con.execute(f"SELECT count(*) FROM {t} WHERE {con.execute(f'DESCRIBE {t}').fetchone()[0]} = -1").fetchone()[0]
        if key == 0:
            con.execute(f"INSERT INTO {t} VALUES {v}")
    # static rows: hospital-ward channel (conformed channel for the hospital demand fact)
    if con.execute("SELECT count(*) FROM dim_channel WHERE sales_channel='Hospital ERP'").fetchone()[0] == 0:
        con.execute("INSERT INTO dim_channel VALUES (nextval('seq_channel_key'),'Hospital ERP','Hospital Pharmacy',"
                    "'Hospital ward','Institutional consumption','HOSPITAL_ERP_FHIR_EXPORT')")
    if con.execute("SELECT count(*) FROM dim_promotion WHERE promotion_code='NO_PROMO'").fetchone()[0] == 0:
        con.execute("INSERT INTO dim_promotion VALUES (nextval('seq_promotion_key'),'NO_PROMO','None',0,FALSE)")
    con.close()


def extract_delta(run_id: str, refresh_tag: str = None) -> dict:
    """Staging rows above the watermark -> CSV hand-off files (one per staging table)."""
    t0 = time.perf_counter()
    marks = dict(pg_query("SELECT staging_table, last_stg_id FROM ops.transform_watermark"))
    manifest = {"run_id": run_id, "refresh_tag": refresh_tag, "tables": {}}
    with pg_conn() as c, c.cursor() as cur:
        for name in MART_SOURCES:
            src = SOURCES[name]
            lo = marks.get(src.table, 0)
            cur.execute(f"SELECT coalesce(max(_stg_id),0) FROM staging.{src.table}")
            hi = cur.fetchone()[0]
            cols = [snake(c) for c, _ in src.columns] + ["_stg_id", "_batch_id", "_refresh_tag", "_ingest_mode",
                                                         "_loaded_at"]
            f = WORK_DIR / run_id / f"{name}.csv"
            f.parent.mkdir(parents=True, exist_ok=True)
            with open(f, "w") as fh:
                cur.copy_expert(f"COPY (SELECT {','.join(cols)} FROM staging.{src.table} WHERE _stg_id > {lo} "
                                f"AND _stg_id <= {hi} ORDER BY _stg_id) TO STDOUT WITH (FORMAT csv, HEADER, NULL '\\N')",
                                fh)
            cur.execute(f"SELECT count(*) FROM staging.{src.table} WHERE _stg_id > {lo} AND _stg_id <= {hi}")
            n = cur.fetchone()[0]
            manifest["tables"][name] = {"file": str(f), "from_id": lo, "to_id": hi, "rows": n,
                                        "staging_table": src.table}
    _manifest_path(run_id).write_text(json.dumps(manifest, indent=2))
    if sum(v["rows"] for v in manifest["tables"].values()) > 0:
        set_mart_state("LOADING", run_id)      # mart will hold rows beyond the committed watermark until reconcile
    log_step(run_id, refresh_tag, "extract_delta", "work/*.csv",
             rows_in=sum(v["rows"] for v in manifest["tables"].values()), t0=t0,
             details=json.dumps({k: v["rows"] for k, v in manifest["tables"].items()}))
    return manifest


def _upsert_geography(con, city_state_sql: str):
    con.execute(f"""
        INSERT INTO dim_geography
        SELECT nextval('seq_geography_key'), g.city, g.state_code, s.state_name, s.census_region, s.census_division, 'US'
        FROM (SELECT DISTINCT city, state_code FROM ({city_state_sql}) WHERE city IS NOT NULL) g
        LEFT JOIN states s USING (state_code)
        WHERE NOT EXISTS (SELECT 1 FROM dim_geography d WHERE d.city = g.city AND d.state_code = g.state_code)""")


@locked
def load_dimensions(run_id: str):
    m = _manifest(run_id)
    tag = m["refresh_tag"]
    con = duck_conn()
    stats = {}
    states = pd.DataFrame([(k, *v) for k, v in STATES.items()],
                          columns=["state_code", "state_name", "census_region", "census_division"])
    con.register("states", states)
    v_master = _delta_view(con, "01_master_data", m)
    v_dl = _delta_view(con, "03_manufacturer_delivery_data", m)
    v_sales = _delta_view(con, "05_pharmacy_sales_data", m)
    v_hosp = _delta_view(con, "06_hospital_demand_data", m)

    # ---- geography (conformed)
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_geography").fetchone()[0]
    _upsert_geography(con, f"SELECT pharmacy_city AS city, pharmacy_state AS state_code FROM {v_sales} "
                           f"UNION SELECT city, state FROM {v_hosp} "
                           f"UNION SELECT destination_city, destination_state FROM {v_dl}")
    stats["dim_geography"] = con.execute("SELECT count(*) FROM dim_geography").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_geography", inserted=stats["dim_geography"], t0=t0)

    # ---- dim_medicine: SCD Type 2 (commercial attributes) + Type 1 (names)
    t0 = time.perf_counter()
    rows = con.execute(f"""
        SELECT medicine_id, internal_sku, gtin14, generic_name, fictional_brand_name AS brand_name, disease_category,
               icd10_category, atc_code, therapeutic_class, dosage_form, strength, route_of_administration, pack_size,
               base_unit_of_measure, prescription_class, controlled_medicine_flag, cold_chain_flag,
               CASE WHEN cold_chain_flag='Y' THEN '2-8 C' ELSE '15-25 C' END AS storage_condition,
               storage_min_temp_c, storage_max_temp_c, max_humidity_pct, shelf_life_months,
               minimum_remaining_shelf_life_days AS min_remaining_shelf_life_days, primary_supplier_id,
               primary_plant_id, manufacturing_lead_time_days, standard_batch_size_packs, safety_stock_days,
               reorder_review_cycle_days, unit_manufacturing_cost_usd, wholesale_price_usd,
               recommended_retail_price_usd,
               CASE WHEN recommended_retail_price_usd < 5 THEN 'Low (<$5)' WHEN recommended_retail_price_usd <= 20
                    THEN 'Mid ($5-20)' ELSE 'High (>$20)' END AS price_band,
               product_status, md5(concat_ws('|', {', '.join(f"CAST({c} AS VARCHAR)" for c in MEDICINE_T2)})) AS scd_hash,
               effective_from_date AS effective_from, _batch_id AS source_batch_id
        FROM {v_master} ORDER BY effective_from_date, medicine_id""").df()
    ins = upd = t1 = 0
    for r in rows.to_dict("records"):
        cur = con.execute("SELECT medicine_key, scd_hash, effective_from FROM dim_medicine "
                          "WHERE medicine_id = ? AND is_current", [r["medicine_id"]]).fetchone()
        cols = [c for c in r]
        if cur is None:                                         # new medicine
            con.execute(f"INSERT INTO dim_medicine (medicine_key, {', '.join(cols)}, effective_to, is_current, "
                        f"version_num) VALUES (nextval('seq_medicine_key'), {', '.join('?' * len(cols))}, "
                        f"DATE '9999-12-31', TRUE, 1)", list(r.values()))
            ins += 1
        elif cur[1] != r["scd_hash"] and pd.Timestamp(r["effective_from"]) > pd.Timestamp(cur[2]):   # Type 2
            con.execute("UPDATE dim_medicine SET is_current = FALSE, effective_to = ?::DATE - 1 "
                        "WHERE medicine_key = ?", [r["effective_from"], cur[0]])
            ver = con.execute("SELECT max(version_num) FROM dim_medicine WHERE medicine_id=?",
                              [r["medicine_id"]]).fetchone()[0]
            con.execute(f"INSERT INTO dim_medicine (medicine_key, {', '.join(cols)}, effective_to, is_current, "
                        f"version_num) VALUES (nextval('seq_medicine_key'), {', '.join('?' * len(cols))}, "
                        f"DATE '9999-12-31', TRUE, ?)", list(r.values()) + [ver + 1])
            upd += 1
        # Type 1: overwrite descriptive attributes on ALL versions
        sets = ", ".join(f"{c} = ?" for c in MEDICINE_T1)
        changed = con.execute(f"SELECT count(*) FROM dim_medicine WHERE medicine_id = ? AND ("
                              + " OR ".join(f"{c} IS DISTINCT FROM ?" for c in MEDICINE_T1) + ")",
                              [r["medicine_id"]] + [r[c] for c in MEDICINE_T1]).fetchone()[0]
        if changed:
            con.execute(f"UPDATE dim_medicine SET {sets} WHERE medicine_id = ?",
                        [r[c] for c in MEDICINE_T1] + [r["medicine_id"]])
            t1 += 1
    log_step(run_id, tag, "load_dimensions", "dim_medicine", rows_in=len(rows), inserted=ins, updated=upd, t0=t0,
             details=f"new={ins} scd2_new_versions={upd} type1_overwrites={t1}")
    stats["dim_medicine"] = dict(new=ins, scd2=upd, type1=t1)

    # ---- dim_hospital: SCD Type 2 on capacity attributes (effective from the demand month)
    t0 = time.perf_counter()
    hrows = con.execute(f"""
        SELECT DISTINCT ON (hospital_id, demand_month)
               hospital_id, hospital_gln, hospital_name, hospital_type, hospital_pharmacy_id, city, state AS state_code,
               licensed_beds, icu_beds, average_occupancy_rate_pct AS avg_occupancy_rate_pct, trauma_level,
               catchment_population,
               CASE WHEN licensed_beds < 400 THEN '<400 beds' WHEN licensed_beds < 700 THEN '400-699 beds'
                    ELSE '700+ beds' END AS bed_size_band,
               md5(concat_ws('|', {', '.join(f"CAST({'average_occupancy_rate_pct' if c == 'avg_occupancy_rate_pct' else c} AS VARCHAR)" for c in HOSPITAL_T2)})) AS scd_hash,
               demand_month AS effective_from
        FROM {v_hosp} WHERE hospital_id IS NOT NULL ORDER BY demand_month, hospital_id""").df()
    hins = hupd = 0
    for r in hrows.to_dict("records"):
        cur = con.execute("SELECT hospital_key, scd_hash, effective_from FROM dim_hospital WHERE hospital_id=? "
                          "AND is_current", [r["hospital_id"]]).fetchone()
        geo = con.execute("SELECT geography_key FROM dim_geography WHERE city=? AND state_code=?",
                          [r["city"], r["state_code"]]).fetchone()
        r["geography_key"] = geo[0] if geo else -1
        cols = list(r)
        if cur is None:
            con.execute(f"INSERT INTO dim_hospital (hospital_key, {', '.join(cols)}, effective_to, is_current, "
                        f"version_num) VALUES (nextval('seq_hospital_key'), {', '.join('?' * len(cols))}, "
                        f"DATE '9999-12-31', TRUE, 1)", list(r.values()))
            hins += 1
        elif cur[1] != r["scd_hash"] and pd.Timestamp(r["effective_from"]) > pd.Timestamp(cur[2]):
            con.execute("UPDATE dim_hospital SET is_current=FALSE, effective_to=?::DATE - 1 WHERE hospital_key=?",
                        [r["effective_from"], cur[0]])
            ver = con.execute("SELECT max(version_num) FROM dim_hospital WHERE hospital_id=?",
                              [r["hospital_id"]]).fetchone()[0]
            con.execute(f"INSERT INTO dim_hospital (hospital_key, {', '.join(cols)}, effective_to, is_current, "
                        f"version_num) VALUES (nextval('seq_hospital_key'), {', '.join('?' * len(cols))}, "
                        f"DATE '9999-12-31', TRUE, ?)", list(r.values()) + [ver + 1])
            hupd += 1
    log_step(run_id, tag, "load_dimensions", "dim_hospital", rows_in=len(hrows), inserted=hins, updated=hupd, t0=t0,
             details=f"new={hins} scd2_new_versions={hupd}")
    stats["dim_hospital"] = dict(new=hins, scd2=hupd)

    # ---- dim_pharmacy: SCD Type 1 (latest attributes win)
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_pharmacy").fetchone()[0]
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ph_new AS
        SELECT DISTINCT ON (pharmacy_id) pharmacy_id, pharmacy_gln, pharmacy_name, pharmacy_type, hospital_id,
               pharmacy_city AS city, pharmacy_state AS state_code
        FROM {v_sales} WHERE pharmacy_id IS NOT NULL ORDER BY pharmacy_id, _stg_id DESC""")
    con.execute("""UPDATE dim_pharmacy d SET pharmacy_gln=n.pharmacy_gln, pharmacy_name=n.pharmacy_name,
                   pharmacy_type=n.pharmacy_type, hospital_id=n.hospital_id, city=n.city, state_code=n.state_code,
                   geography_key=coalesce(g.geography_key,-1), updated_at=now()
                   FROM ph_new n LEFT JOIN dim_geography g ON g.city=n.city AND g.state_code=n.state_code
                   WHERE d.pharmacy_id=n.pharmacy_id AND (d.pharmacy_name IS DISTINCT FROM n.pharmacy_name OR
                         d.pharmacy_type IS DISTINCT FROM n.pharmacy_type OR d.city IS DISTINCT FROM n.city)""")
    con.execute("""INSERT INTO dim_pharmacy SELECT nextval('seq_pharmacy_key'), n.pharmacy_id, n.pharmacy_gln,
                   n.pharmacy_name, n.pharmacy_type, n.hospital_id, n.city, n.state_code, coalesce(g.geography_key,-1),
                   now() FROM ph_new n LEFT JOIN dim_geography g ON g.city=n.city AND g.state_code=n.state_code
                   WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy d WHERE d.pharmacy_id = n.pharmacy_id)""")
    stats["dim_pharmacy"] = con.execute("SELECT count(*) FROM dim_pharmacy").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_pharmacy", inserted=stats["dim_pharmacy"], t0=t0)

    # ---- dim_warehouse (from manufacturer deliveries; ids seen only in sales get an id-only row)
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_warehouse").fetchone()[0]
    con.execute(f"""INSERT INTO dim_warehouse
        SELECT nextval('seq_warehouse_key'), w.destination_warehouse_id, w.destination_warehouse_gln,
               w.destination_warehouse_name, w.destination_city, w.destination_state, coalesce(g.geography_key,-1)
        FROM (SELECT DISTINCT ON (destination_warehouse_id) * FROM {v_dl} ORDER BY destination_warehouse_id) w
        LEFT JOIN dim_geography g ON g.city = w.destination_city AND g.state_code = w.destination_state
        WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse d WHERE d.warehouse_id = w.destination_warehouse_id)""")
    con.execute(f"""INSERT INTO dim_warehouse
        SELECT nextval('seq_warehouse_key'), s.source_warehouse_id, NULL, s.source_warehouse_id, NULL, NULL, -1
        FROM (SELECT DISTINCT source_warehouse_id FROM {v_sales} WHERE source_warehouse_id IS NOT NULL) s
        WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse d WHERE d.warehouse_id = s.source_warehouse_id)""")
    stats["dim_warehouse"] = con.execute("SELECT count(*) FROM dim_warehouse").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_warehouse", inserted=stats["dim_warehouse"], t0=t0)

    # ---- dim_channel (Retail pharmacy / Hospital pharmacy / E-commerce + static Hospital ward)
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_channel").fetchone()[0]
    con.execute(f"""INSERT INTO dim_channel
        SELECT nextval('seq_channel_key'), sales_channel, pharmacy_type,
               CASE WHEN sales_channel = 'Ecommerce' THEN 'E-commerce'
                    WHEN pharmacy_type = 'Hospital Pharmacy' THEN 'Hospital pharmacy' ELSE 'Retail pharmacy' END,
               'Commercial sale', source_system
        FROM (SELECT DISTINCT sales_channel, pharmacy_type, source_system FROM {v_sales}) s
        WHERE NOT EXISTS (SELECT 1 FROM dim_channel d WHERE d.sales_channel = s.sales_channel
                          AND d.pharmacy_type = s.pharmacy_type)""")
    stats["dim_channel"] = con.execute("SELECT count(*) FROM dim_channel").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_channel", inserted=stats["dim_channel"], t0=t0)

    # ---- dim_promotion
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_promotion").fetchone()[0]
    con.execute(f"""INSERT INTO dim_promotion
        SELECT nextval('seq_promotion_key'), p,
               CASE WHEN p LIKE 'LOYALTY%' THEN 'Loyalty' WHEN p LIKE 'SEASON%' THEN 'Seasonal'
                    WHEN p LIKE 'ONLINE%' THEN 'Online' WHEN p LIKE 'WELLNESS%' THEN 'Wellness' ELSE 'Other' END,
               TRY_CAST(regexp_extract(p, '(\\d+)$', 1) AS DOUBLE), TRUE
        FROM (SELECT DISTINCT promotion_code AS p FROM {v_sales} WHERE promotion_code IS NOT NULL) s
        WHERE NOT EXISTS (SELECT 1 FROM dim_promotion d WHERE d.promotion_code = s.p)""")
    stats["dim_promotion"] = con.execute("SELECT count(*) FROM dim_promotion").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_promotion", inserted=stats["dim_promotion"], t0=t0)

    # ---- junk dimensions
    t0 = time.perf_counter()
    before = con.execute("SELECT count(*) FROM dim_sales_profile").fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TEMP VIEW sales_profile_src AS
        SELECT customer_segment, payer_type, payment_method, prescription_required_flag, ecommerce_delivery_status,
               return_flag, sale_status,
               CASE WHEN month(sales_date) = 1 AND day(sales_date) = 1
                         AND date_diff('day', pharmacy_delivery_date, sales_date) > {BULK_POSTING_GAP_DAYS}
                    THEN 'Year-start bulk posting' ELSE 'Normal' END AS date_quality_flag
        FROM {v_sales}""")
    con.execute("""INSERT INTO dim_sales_profile
        SELECT nextval('seq_sales_profile_key'), * FROM (SELECT DISTINCT * FROM sales_profile_src) s
        WHERE NOT EXISTS (SELECT 1 FROM dim_sales_profile d WHERE
              d.customer_segment IS NOT DISTINCT FROM s.customer_segment AND d.payer_type IS NOT DISTINCT FROM s.payer_type
          AND d.payment_method IS NOT DISTINCT FROM s.payment_method
          AND d.prescription_required_flag IS NOT DISTINCT FROM s.prescription_required_flag
          AND d.ecommerce_delivery_status IS NOT DISTINCT FROM s.ecommerce_delivery_status
          AND d.return_flag IS NOT DISTINCT FROM s.return_flag AND d.sale_status IS NOT DISTINCT FROM s.sale_status
          AND d.date_quality_flag IS NOT DISTINCT FROM s.date_quality_flag)""")
    stats["dim_sales_profile"] = con.execute("SELECT count(*) FROM dim_sales_profile").fetchone()[0] - before
    before = con.execute("SELECT count(*) FROM dim_demand_profile").fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TEMP VIEW demand_profile_src AS
        SELECT medication_request_status, formulary_status,
               CASE WHEN emergency_request_quantity_packs > 0 THEN 'Y' ELSE 'N' END AS emergency_request_flag,
               CASE WHEN stockout_days > 0 THEN 'Y' ELSE 'N' END AS stockout_flag,
               CASE WHEN unfulfilled_demand_packs > 0 THEN 'Y' ELSE 'N' END AS unfulfilled_flag
        FROM {v_hosp}""")
    con.execute("""INSERT INTO dim_demand_profile
        SELECT nextval('seq_demand_profile_key'), * FROM (SELECT DISTINCT * FROM demand_profile_src) s
        WHERE NOT EXISTS (SELECT 1 FROM dim_demand_profile d WHERE
              d.medication_request_status IS NOT DISTINCT FROM s.medication_request_status
          AND d.formulary_status IS NOT DISTINCT FROM s.formulary_status
          AND d.emergency_request_flag = s.emergency_request_flag AND d.stockout_flag = s.stockout_flag
          AND d.unfulfilled_flag = s.unfulfilled_flag)""")
    stats["dim_demand_profile"] = con.execute("SELECT count(*) FROM dim_demand_profile").fetchone()[0] - before
    log_step(run_id, tag, "load_dimensions", "dim_sales_profile+dim_demand_profile",
             inserted=stats["dim_sales_profile"] + stats["dim_demand_profile"], t0=t0,
             details=f"sales_profile={stats['dim_sales_profile']} demand_profile={stats['dim_demand_profile']}")
    con.close()
    return stats


@locked
def load_fact_sales(run_id: str) -> int:
    m = _manifest(run_id)
    t0 = time.perf_counter()
    con = duck_conn()
    v = _delta_view(con, "05_pharmacy_sales_data", m)
    before = con.execute("SELECT count(*) FROM fact_sales").fetchone()[0]
    con.execute(f"""
        INSERT INTO fact_sales
        WITH s AS (
            SELECT *, CASE WHEN month(sales_date) = 1 AND day(sales_date) = 1
                             AND date_diff('day', pharmacy_delivery_date, sales_date) > {BULK_POSTING_GAP_DAYS}
                           THEN 'Year-start bulk posting' ELSE 'Normal' END AS date_quality_flag
            FROM {v}),
        first_version AS (SELECT medicine_id, arg_min(medicine_key, effective_from) AS medicine_key
                          FROM dim_medicine GROUP BY medicine_id)
        SELECT s.sales_transaction_id,
               CAST(strftime(s.sales_date, '%Y%m%d') AS INT),
               CAST(strftime(s.pharmacy_delivery_date, '%Y%m%d') AS INT),
               CAST(strftime(s.batch_expiry_date, '%Y%m%d') AS INT),
               coalesce(m.medicine_key, fv.medicine_key, -1),
               coalesce(p.pharmacy_key, -1), coalesce(p.geography_key, -1), coalesce(c.channel_key, -1),
               coalesce(pr.promotion_key, -1), coalesce(sp.sales_profile_key, -1), coalesce(w.warehouse_key, -1),
               s.pharmacy_order_id, s.pharmacy_delivery_id, s.prescription_id, s.warehouse_lot_id,
               s.production_batch_id, s.sales_timestamp_utc AT TIME ZONE 'UTC',
               hour(s.sales_timestamp_utc AT TIME ZONE 'UTC'),
               s.quantity_sold_packs, s.unit_retail_price_usd, s.gross_sales_amount_usd, s.discount_amount_usd,
               s.net_sales_amount_usd, s.sales_tax_usd, s.cost_of_goods_sold_usd, s.gross_margin_usd,
               s.returned_quantity_packs, s.days_to_expiry_at_sale, s.delivery_quantity_packs, s.delivery_otif_flag,
               s._ingest_mode, s._refresh_tag, s._batch_id, now()
        FROM s
        LEFT JOIN dim_medicine m ON m.medicine_id = s.medicine_id
                                AND s.sales_date BETWEEN m.effective_from AND m.effective_to
        LEFT JOIN first_version fv ON fv.medicine_id = s.medicine_id
        LEFT JOIN dim_pharmacy p ON p.pharmacy_id = s.pharmacy_id
        LEFT JOIN dim_channel c ON c.sales_channel = s.sales_channel AND c.pharmacy_type = s.pharmacy_type
        LEFT JOIN dim_promotion pr ON pr.promotion_code = coalesce(s.promotion_code, 'NO_PROMO')
        LEFT JOIN dim_warehouse w ON w.warehouse_id = s.source_warehouse_id
        LEFT JOIN dim_sales_profile sp ON sp.customer_segment IS NOT DISTINCT FROM s.customer_segment
             AND sp.payer_type IS NOT DISTINCT FROM s.payer_type AND sp.payment_method IS NOT DISTINCT FROM s.payment_method
             AND sp.prescription_required_flag IS NOT DISTINCT FROM s.prescription_required_flag
             AND sp.ecommerce_delivery_status IS NOT DISTINCT FROM s.ecommerce_delivery_status
             AND sp.return_flag IS NOT DISTINCT FROM s.return_flag AND sp.sale_status IS NOT DISTINCT FROM s.sale_status
             AND sp.date_quality_flag = s.date_quality_flag
        WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.sales_transaction_id = s.sales_transaction_id)""")
    n = con.execute("SELECT count(*) FROM fact_sales").fetchone()[0] - before
    con.close()
    log_step(run_id, m["refresh_tag"], "load_fact_sales", "fact_sales",
             rows_in=m["tables"]["05_pharmacy_sales_data"]["rows"], inserted=n, t0=t0)
    return n


@locked
def load_fact_hospital_demand(run_id: str) -> int:
    m = _manifest(run_id)
    t0 = time.perf_counter()
    con = duck_conn()
    v = _delta_view(con, "06_hospital_demand_data", m)
    before = con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]
    con.execute(f"""
        INSERT INTO fact_hospital_demand
        WITH first_version AS (SELECT medicine_id, arg_min(medicine_key, effective_from) AS medicine_key
                               FROM dim_medicine GROUP BY medicine_id),
             ward AS (SELECT channel_key FROM dim_channel WHERE sales_channel = 'Hospital ERP')
        SELECT h.hospital_demand_record_id, CAST(strftime(h.demand_month, '%Y%m%d') AS INT),
               coalesce(dh.hospital_key, -1), coalesce(m.medicine_key, fv.medicine_key, -1),
               coalesce(dh.geography_key, -1), (SELECT channel_key FROM ward), coalesce(dp.demand_profile_key, -1),
               h.estimated_disease_cases, h.disease_related_admissions, h.outpatient_encounters,
               h.forecast_demand_packs, h.requested_quantity_packs, h.approved_quantity_packs,
               h.received_quantity_packs, h.emergency_request_quantity_packs, h.opening_stock_packs,
               h.dispensed_quantity_packs, h.consumed_quantity_packs, h.wastage_quantity_packs, h.closing_stock_packs,
               h.unfulfilled_demand_packs, h.stockout_days, h.service_level_pct, h.average_lead_time_days,
               h.demand_trend_index,
               h.forecast_demand_packs - h.consumed_quantity_packs,
               abs(h.forecast_demand_packs - h.consumed_quantity_packs),
               h.requested_quantity_packs - h.received_quantity_packs,
               h._refresh_tag, h._batch_id, now()
        FROM {v} h
        LEFT JOIN dim_hospital dh ON dh.hospital_id = h.hospital_id
                                 AND h.demand_month BETWEEN dh.effective_from AND dh.effective_to
        LEFT JOIN dim_medicine m ON m.medicine_id = h.medicine_id
                                AND h.demand_month BETWEEN m.effective_from AND m.effective_to
        LEFT JOIN first_version fv ON fv.medicine_id = h.medicine_id
        LEFT JOIN dim_demand_profile dp
               ON dp.medication_request_status IS NOT DISTINCT FROM h.medication_request_status
              AND dp.formulary_status IS NOT DISTINCT FROM h.formulary_status
              AND dp.emergency_request_flag = CASE WHEN h.emergency_request_quantity_packs > 0 THEN 'Y' ELSE 'N' END
              AND dp.stockout_flag = CASE WHEN h.stockout_days > 0 THEN 'Y' ELSE 'N' END
              AND dp.unfulfilled_flag = CASE WHEN h.unfulfilled_demand_packs > 0 THEN 'Y' ELSE 'N' END
        WHERE NOT EXISTS (SELECT 1 FROM fact_hospital_demand f
                          WHERE f.hospital_demand_record_id = h.hospital_demand_record_id)""")
    n = con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0] - before
    con.close()
    log_step(run_id, m["refresh_tag"], "load_fact_hospital_demand", "fact_hospital_demand",
             rows_in=m["tables"]["06_hospital_demand_data"]["rows"], inserted=n, t0=t0)
    return n


AGGREGATES_SQL = {
    # pre-aggregated summary tables (DuckDB has no materialized views -> refreshed by Airflow after every load)
    "agg_sales_monthly": """
        CREATE OR REPLACE TABLE agg_sales_monthly AS
        SELECT d.year_month, d.year_num, d.month_num, m.medicine_id, m.generic_name, m.therapeutic_class,
               m.disease_category, m.cold_chain_flag, c.channel_group, g.census_region, g.state_code,
               sp.date_quality_flag,
               count(*) AS txn_count, sum(f.quantity_sold_packs) AS qty_packs, sum(f.gross_sales_usd) AS gross_usd,
               sum(f.discount_usd) AS discount_usd, sum(f.net_sales_usd) AS net_usd, sum(f.cogs_usd) AS cogs_usd,
               sum(f.gross_margin_usd) AS margin_usd, sum(f.returned_qty_packs) AS returned_packs,
               count(*) FILTER (WHERE pr.is_promoted) AS promoted_txn,
               count(*) FILTER (WHERE sp.ecommerce_delivery_status = 'Returned to sender') AS rts_txn,
               count(*) FILTER (WHERE sp.ecommerce_delivery_status <> 'Not applicable') AS ecom_txn
        FROM fact_sales f
        JOIN dim_date d ON d.date_key = f.sale_date_key
        JOIN dim_medicine m ON m.medicine_key = f.medicine_key
        JOIN dim_channel c ON c.channel_key = f.channel_key
        JOIN dim_geography g ON g.geography_key = f.geography_key
        JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
        JOIN dim_promotion pr ON pr.promotion_key = f.promotion_key
        GROUP BY ALL ORDER BY year_month, medicine_id""",
    "agg_sales_daily_channel": """
        CREATE OR REPLACE TABLE agg_sales_daily_channel AS
        SELECT f.sale_date_key, d.full_date, d.year_month, c.channel_group, sp.date_quality_flag,
               count(*) AS txn_count, sum(f.quantity_sold_packs) AS qty_packs, sum(f.net_sales_usd) AS net_usd,
               sum(f.gross_margin_usd) AS margin_usd
        FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key
        JOIN dim_channel c ON c.channel_key = f.channel_key
        JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
        GROUP BY ALL ORDER BY sale_date_key""",
    "agg_hospital_monthly": """
        CREATE OR REPLACE TABLE agg_hospital_monthly AS
        SELECT d.year_month, d.year_num, h.hospital_id, h.hospital_name, h.hospital_type, g.census_region,
               m.disease_category,
               count(*) AS records, sum(forecast_demand_packs) AS forecast_packs,
               sum(consumed_qty_packs) AS consumed_packs, sum(requested_qty_packs) AS requested_packs,
               sum(approved_qty_packs) AS approved_packs, sum(received_qty_packs) AS received_packs,
               sum(dispensed_qty_packs) AS dispensed_packs, sum(emergency_request_packs) AS emergency_packs,
               sum(unfulfilled_demand_packs) AS unfulfilled_packs, sum(stockout_days) AS stockout_days,
               sum(forecast_error_packs) AS forecast_error_packs, sum(abs_forecast_error_packs) AS abs_error_packs,
               sum(wastage_qty_packs) AS wastage_packs
        FROM fact_hospital_demand f
        JOIN dim_date d ON d.date_key = f.demand_month_key
        JOIN dim_hospital h ON h.hospital_key = f.hospital_key
        JOIN dim_medicine m ON m.medicine_key = f.medicine_key
        JOIN dim_geography g ON g.geography_key = f.geography_key
        GROUP BY ALL ORDER BY year_month, hospital_id""",
    "agg_unified_demand_monthly": """
        CREATE OR REPLACE TABLE agg_unified_demand_monthly AS
        SELECT year_month, medicine_id, generic_name, therapeutic_class, disease_category, cold_chain_flag,
               census_region, channel_group, sum(demand_packs) AS demand_packs, sum(net_usd) AS net_usd,
               sum(records) AS records
        FROM (
            SELECT year_month, medicine_id, generic_name, therapeutic_class, disease_category, cold_chain_flag,
                   census_region, channel_group, qty_packs AS demand_packs, net_usd, txn_count AS records
            FROM agg_sales_monthly WHERE date_quality_flag = 'Normal'
            UNION ALL
            SELECT d.year_month, m.medicine_id, m.generic_name, m.therapeutic_class, m.disease_category,
                   m.cold_chain_flag, g.census_region, 'Hospital ward', f.consumed_qty_packs, NULL, 1
            FROM fact_hospital_demand f JOIN dim_date d ON d.date_key = f.demand_month_key
            JOIN dim_medicine m ON m.medicine_key = f.medicine_key
            JOIN dim_geography g ON g.geography_key = f.geography_key)
        GROUP BY ALL ORDER BY year_month, medicine_id""",
    "agg_medicine_velocity": """
        CREATE OR REPLACE TABLE agg_medicine_velocity AS
        WITH s AS (
            SELECT medicine_id, generic_name, therapeutic_class, disease_category, cold_chain_flag,
                   sum(qty_packs) AS packs, sum(net_usd) AS net_usd, sum(margin_usd) AS margin_usd,
                   count(DISTINCT year_month) AS active_months, min(year_month) AS first_month,
                   max(year_month) AS last_month
            FROM agg_sales_monthly WHERE date_quality_flag = 'Normal' GROUP BY ALL)
        SELECT *, packs / active_months AS packs_per_month,
               rank() OVER (PARTITION BY therapeutic_class ORDER BY packs / active_months DESC) AS rank_in_class,
               rank() OVER (ORDER BY packs / active_months DESC) AS overall_rank,
               CASE ntile(3) OVER (ORDER BY packs / active_months DESC) WHEN 1 THEN 'Fast' WHEN 2 THEN 'Medium'
                    ELSE 'Slow' END AS velocity_class
        FROM s""",
}


@locked
def build_aggregates(run_id: str) -> dict:
    m = _manifest(run_id)
    con = duck_conn()
    out = {}
    for name, sql in AGGREGATES_SQL.items():
        t0 = time.perf_counter()
        con.execute(sql)
        out[name] = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        log_step(run_id, m["refresh_tag"], "build_aggregates", name, inserted=out[name], t0=t0)
    con.close()
    return out


@locked
def reconcile_and_commit(run_id: str) -> dict:
    """Staging vs mart reconciliation. Watermarks advance only if every check passes."""
    m = _manifest(run_id)
    t0 = time.perf_counter()
    con = duck_conn(read_only=True)
    checks = {}
    stg_sales = pg_query("SELECT count(*), round(sum(net_sales_amount_usd)::numeric, 2) FROM staging.stg_pharmacy_sales_data "
                         "WHERE _stg_id <= %s", (m["tables"]["05_pharmacy_sales_data"]["to_id"],))[0]
    mart_sales = con.execute("SELECT count(*), round(sum(net_sales_usd), 2) FROM fact_sales").fetchone()
    checks["sales_row_count"] = (int(stg_sales[0]), int(mart_sales[0]))
    checks["sales_net_usd"] = (float(stg_sales[1] or 0), float(mart_sales[1] or 0))
    stg_h = pg_query("SELECT count(*), sum(consumed_quantity_packs) FROM staging.stg_hospital_demand_data "
                     "WHERE _stg_id <= %s", (m["tables"]["06_hospital_demand_data"]["to_id"],))[0]
    mart_h = con.execute("SELECT count(*), sum(consumed_qty_packs) FROM fact_hospital_demand").fetchone()
    checks["hospital_row_count"] = (int(stg_h[0]), int(mart_h[0]))
    checks["hospital_consumed_packs"] = (int(stg_h[1] or 0), int(mart_h[1] or 0))
    checks["agg_vs_fact_net_usd"] = (float(mart_sales[1] or 0),
                                     float(con.execute("SELECT round(sum(net_usd),2) FROM agg_sales_monthly").fetchone()[0] or 0))
    orphans = {}
    for col in ["medicine_key", "pharmacy_key", "channel_key", "promotion_key", "sales_profile_key", "geography_key",
                "warehouse_key"]:
        orphans[f"fact_sales.{col}"] = con.execute(f"SELECT count(*) FROM fact_sales WHERE {col} = -1").fetchone()[0]
    for col in ["hospital_key", "medicine_key", "demand_profile_key"]:
        orphans[f"fact_hospital_demand.{col}"] = con.execute(
            f"SELECT count(*) FROM fact_hospital_demand WHERE {col} = -1").fetchone()[0]
    con.close()
    failed = [k for k, (a, b) in checks.items() if abs(a - b) > 0.05]
    status = "SUCCESS" if not failed else "FAILED"
    details = json.dumps({"checks": checks, "unknown_member_rows": orphans, "failed": failed})
    log_step(run_id, m["refresh_tag"], "reconcile", "fact_sales,fact_hospital_demand", t0=t0, status=status,
             details=details)
    if failed:
        raise RuntimeError(f"Reconciliation failed: {failed} -> {checks}")
    with pg_conn() as c, c.cursor() as cur:      # watermark + READY flag commit atomically
        for name, info in m["tables"].items():
            cur.execute("UPDATE ops.transform_watermark SET last_stg_id=%s, updated_at=now() WHERE staging_table=%s",
                        (info["to_id"], info["staging_table"]))
        set_mart_state("READY", run_id, cur=cur)
    return {"checks": checks, "unknown_member_rows": orphans}


def run_all(run_id: str = None, refresh_tag: str = None):
    run_id = run_id or f"T-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}"
    init_mart()
    extract_delta(run_id, refresh_tag)
    load_dimensions(run_id)
    load_fact_sales(run_id)
    load_fact_hospital_demand(run_id)
    build_aggregates(run_id)
    return reconcile_and_commit(run_id)
