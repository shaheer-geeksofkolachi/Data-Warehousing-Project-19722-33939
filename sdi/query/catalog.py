"""Analytical query catalog for the SDI mart.

Every business question is implemented three times so the Query dashboard can show WHY the warehouse exists:
  oltp_staging    Postgres staging (row store, source-shaped tables, joins/CASE logic at query time)
  star_schema     DuckDB star schema, joins from base facts to dimensions
  star_optimized  DuckDB + optimisation: pre-aggregated summary tables (materialised-view equivalents),
                  junk-dimension pre-filters, hive-partitioned Parquet with partition pruning
All three tiers return the same answer (checked by the benchmark's result-equivalence test).
"""
from ..warehouse.transform import STATES
from ..config import PARQUET_DIR

_REGION_CASE = "CASE " + " ".join(
    f"WHEN {{col}} IN ({', '.join(repr(s) for s, v in STATES.items() if v[1] == r)}) THEN '{r}'"
    for r in ("Northeast", "Midwest", "South", "West")) + " ELSE 'Unknown' END"


def region_case(col):
    return _REGION_CASE.replace("{col}", col)


PG_CHANNEL = ("CASE WHEN sales_channel='Ecommerce' THEN 'E-commerce' WHEN pharmacy_type='Hospital Pharmacy' "
              "THEN 'Hospital pharmacy' ELSE 'Retail pharmacy' END")
PG_NORMAL = ("NOT (extract(month from sales_date)=1 AND extract(day from sales_date)=1 "
             "AND sales_date - pharmacy_delivery_date > 240)")
PARQUET = f"read_parquet('{PARQUET_DIR}/fact_sales/*/*/*.parquet', hive_partitioning = true)"

DASH_HOSP = "BI-1 Hospital Forecast vs Actual & Unfulfilled Demand"
DASH_CHAN = "BI-2 Channel Revenue Split & Gross Margin"
DASH_VEL = "BI-3 Fast vs Slow-moving Medicines (Cold Chain)"
DASH_DEM = "BI-4 Unified Demand View"

QUERIES = [
    dict(id="Q01", widget="Forecast vs actual consumption (monthly)", dashboard=DASH_HOSP,
         question="Are hospital forecasts keeping up with actual consumption month by month?",
         optimization="Pre-aggregated agg_hospital_monthly (3k rows instead of 27k-row fact + 2 joins)",
         sql={
             "oltp_staging": """
                SELECT to_char(demand_month,'YYYY-MM') AS year_month, sum(forecast_demand_packs) AS forecast,
                       sum(consumed_quantity_packs) AS consumed,
                       round(sum(forecast_demand_packs)::numeric / nullif(sum(consumed_quantity_packs),0), 4) AS ratio,
                       round(100.0*(sum(forecast_demand_packs)-sum(consumed_quantity_packs))/nullif(sum(consumed_quantity_packs),0),2) AS bias_pct
                FROM staging.stg_hospital_demand_data GROUP BY 1 ORDER BY 1""",
             "star_schema": """
                SELECT d.year_month, sum(f.forecast_demand_packs) AS forecast, sum(f.consumed_qty_packs) AS consumed,
                       round(sum(f.forecast_demand_packs) / nullif(sum(f.consumed_qty_packs),0), 4) AS ratio,
                       round(100.0*sum(f.forecast_error_packs)/nullif(sum(f.consumed_qty_packs),0),2) AS bias_pct
                FROM fact_hospital_demand f JOIN dim_date d ON d.date_key = f.demand_month_key
                JOIN dim_hospital h ON h.hospital_key = f.hospital_key
                GROUP BY 1 ORDER BY 1""",
             "star_optimized": """
                SELECT year_month, sum(forecast_packs) AS forecast, sum(consumed_packs) AS consumed,
                       round(sum(forecast_packs) / nullif(sum(consumed_packs),0), 4) AS ratio,
                       round(100.0*sum(forecast_error_packs)/nullif(sum(consumed_packs),0),2) AS bias_pct
                FROM agg_hospital_monthly GROUP BY 1 ORDER BY 1"""}),
    dict(id="Q02", widget="Forecast accuracy by disease x hospital (WAPE, bias)", dashboard=DASH_HOSP,
         question="Which hospitals and disease categories have the least accurate forecasts?",
         optimization="Pre-aggregated agg_hospital_monthly; dimension attributes denormalised into the summary",
         sql={
             "oltp_staging": """
                SELECT disease_category, hospital_name, sum(consumed_quantity_packs) AS consumed,
                       round(100.0*sum(abs(forecast_demand_packs-consumed_quantity_packs))/nullif(sum(consumed_quantity_packs),0),2) AS wape_pct,
                       round(100.0*sum(forecast_demand_packs-consumed_quantity_packs)/nullif(sum(consumed_quantity_packs),0),2) AS bias_pct
                FROM staging.stg_hospital_demand_data GROUP BY 1,2 ORDER BY wape_pct DESC, 1, 2 LIMIT 15""",
             "star_schema": """
                SELECT m.disease_category, h.hospital_name, sum(f.consumed_qty_packs) AS consumed,
                       round(100.0*sum(f.abs_forecast_error_packs)/nullif(sum(f.consumed_qty_packs),0),2) AS wape_pct,
                       round(100.0*sum(f.forecast_error_packs)/nullif(sum(f.consumed_qty_packs),0),2) AS bias_pct
                FROM fact_hospital_demand f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                JOIN dim_hospital h ON h.hospital_key = f.hospital_key
                GROUP BY 1,2 ORDER BY wape_pct DESC, 1, 2 LIMIT 15""",
             "star_optimized": """
                SELECT disease_category, hospital_name, sum(consumed_packs) AS consumed,
                       round(100.0*sum(abs_error_packs)/nullif(sum(consumed_packs),0),2) AS wape_pct,
                       round(100.0*sum(forecast_error_packs)/nullif(sum(consumed_packs),0),2) AS bias_pct
                FROM agg_hospital_monthly GROUP BY 1,2 ORDER BY wape_pct DESC, 1, 2 LIMIT 15"""}),
    dict(id="Q03", widget="Supply-gap funnel by hospital (requested > approved > received)", dashboard=DASH_HOSP,
         question="Where is hospital demand lost between request, approval and receipt?",
         optimization="Pre-aggregated agg_hospital_monthly",
         sql={
             "oltp_staging": """
                SELECT hospital_name, sum(requested_quantity_packs) AS requested, sum(approved_quantity_packs) AS approved,
                       sum(received_quantity_packs) AS received, sum(consumed_quantity_packs) AS consumed,
                       round(100.0*sum(received_quantity_packs)/nullif(sum(requested_quantity_packs),0),2) AS fill_rate_pct,
                       sum(requested_quantity_packs)-sum(received_quantity_packs) AS gap_packs
                FROM staging.stg_hospital_demand_data GROUP BY 1 ORDER BY gap_packs DESC""",
             "star_schema": """
                SELECT h.hospital_name, sum(requested_qty_packs) AS requested, sum(approved_qty_packs) AS approved,
                       sum(received_qty_packs) AS received, sum(consumed_qty_packs) AS consumed,
                       round(100.0*sum(received_qty_packs)/nullif(sum(requested_qty_packs),0),2) AS fill_rate_pct,
                       sum(supply_gap_packs) AS gap_packs
                FROM fact_hospital_demand f JOIN dim_hospital h ON h.hospital_key = f.hospital_key
                GROUP BY 1 ORDER BY gap_packs DESC""",
             "star_optimized": """
                SELECT hospital_name, sum(requested_packs) AS requested, sum(approved_packs) AS approved,
                       sum(received_packs) AS received, sum(consumed_packs) AS consumed,
                       round(100.0*sum(received_packs)/nullif(sum(requested_packs),0),2) AS fill_rate_pct,
                       sum(requested_packs)-sum(received_packs) AS gap_packs
                FROM agg_hospital_monthly GROUP BY 1 ORDER BY gap_packs DESC"""}),
    dict(id="Q04", widget="Unfulfilled & emergency demand hotspots (hospital x therapeutic class)", dashboard=DASH_HOSP,
         question="Which hospital / therapeutic-class combinations generate unfulfilled and emergency demand?",
         optimization="Junk-dimension pre-filter (dim_demand_profile: unfulfilled/emergency/stockout flags) "
                      "prunes ~89% of fact rows before the joins",
         sql={
             "oltp_staging": """
                SELECT d.hospital_name, m.therapeutic_class, sum(d.unfulfilled_demand_packs) AS unfulfilled,
                       sum(d.emergency_request_quantity_packs) AS emergency, sum(d.stockout_days) AS stockout_days
                FROM staging.stg_hospital_demand_data d
                JOIN (SELECT DISTINCT ON (medicine_id) medicine_id, therapeutic_class FROM staging.stg_master_data
                      ORDER BY medicine_id, effective_from_date DESC) m USING (medicine_id)
                GROUP BY 1,2 HAVING sum(d.unfulfilled_demand_packs) + sum(d.emergency_request_quantity_packs) > 0
                ORDER BY unfulfilled DESC, emergency DESC, 1, 2 LIMIT 10""",
             "star_schema": """
                SELECT h.hospital_name, m.therapeutic_class, sum(f.unfulfilled_demand_packs) AS unfulfilled,
                       sum(f.emergency_request_packs) AS emergency, sum(f.stockout_days) AS stockout_days
                FROM fact_hospital_demand f JOIN dim_hospital h ON h.hospital_key = f.hospital_key
                JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                GROUP BY 1,2 HAVING sum(f.unfulfilled_demand_packs) + sum(f.emergency_request_packs) > 0
                ORDER BY unfulfilled DESC, emergency DESC, 1, 2 LIMIT 10""",
             "star_optimized": """
                SELECT h.hospital_name, m.therapeutic_class, sum(f.unfulfilled_demand_packs) AS unfulfilled,
                       sum(f.emergency_request_packs) AS emergency, sum(f.stockout_days) AS stockout_days
                FROM fact_hospital_demand f
                JOIN dim_demand_profile p ON p.demand_profile_key = f.demand_profile_key
                     AND (p.unfulfilled_flag = 'Y' OR p.emergency_request_flag = 'Y' OR p.stockout_flag = 'Y')
                JOIN dim_hospital h ON h.hospital_key = f.hospital_key
                JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                GROUP BY 1,2 HAVING sum(f.unfulfilled_demand_packs) + sum(f.emergency_request_packs) > 0
                ORDER BY unfulfilled DESC, emergency DESC, 1, 2 LIMIT 10"""}),
    dict(id="Q05", widget="Channel revenue split & gross margin by year", dashboard=DASH_CHAN,
         question="How do Retail, Hospital-pharmacy and E-commerce compare on revenue, volume and margin?",
         optimization="agg_sales_monthly (30k rows instead of 530k-row fact + 4 joins)",
         sql={
             "oltp_staging": f"""
                SELECT extract(year from sales_date)::int AS year, {PG_CHANNEL} AS channel_group, count(*) AS txns,
                       sum(quantity_sold_packs) AS packs, round(sum(net_sales_amount_usd)::numeric,2) AS net_usd,
                       round(sum(gross_margin_usd)::numeric,2) AS margin_usd,
                       round(100*sum(gross_margin_usd)::numeric/nullif(sum(net_sales_amount_usd),0)::numeric,2) AS margin_pct
                FROM staging.stg_pharmacy_sales_data WHERE {PG_NORMAL} GROUP BY 1,2 ORDER BY 1,2""",
             "star_schema": """
                SELECT d.year_num AS year, c.channel_group, count(*) AS txns, sum(f.quantity_sold_packs) AS packs,
                       round(sum(f.net_sales_usd),2) AS net_usd, round(sum(f.gross_margin_usd),2) AS margin_usd,
                       round(100*sum(f.gross_margin_usd)/nullif(sum(f.net_sales_usd),0),2) AS margin_pct
                FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key
                JOIN dim_channel c ON c.channel_key = f.channel_key
                JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                WHERE sp.date_quality_flag = 'Normal' GROUP BY 1,2 ORDER BY 1,2""",
             "star_optimized": """
                SELECT year_num AS year, channel_group, sum(txn_count) AS txns, sum(qty_packs) AS packs,
                       round(sum(net_usd),2) AS net_usd, round(sum(margin_usd),2) AS margin_usd,
                       round(100*sum(margin_usd)/nullif(sum(net_usd),0),2) AS margin_pct
                FROM agg_sales_monthly WHERE date_quality_flag = 'Normal' GROUP BY 1,2 ORDER BY 1,2"""}),
    dict(id="Q06", widget="Monthly revenue trend by channel with YoY growth", dashboard=DASH_CHAN,
         question="Which channel is growing fastest, month over month and year over year?",
         optimization="agg_sales_daily_channel (3k rows) + window function on the small aggregate",
         sql={
             "oltp_staging": f"""
                WITH m AS (SELECT to_char(sales_date,'YYYY-MM') AS year_month, {PG_CHANNEL} AS channel_group,
                                  sum(net_sales_amount_usd) AS net_usd
                           FROM staging.stg_pharmacy_sales_data WHERE {PG_NORMAL} GROUP BY 1,2)
                SELECT year_month, channel_group, round(net_usd::numeric,2) AS net_usd,
                       round((100*(net_usd / nullif(lag(net_usd,12) OVER (PARTITION BY channel_group ORDER BY year_month),0) - 1))::numeric,2) AS yoy_pct
                FROM m ORDER BY 2,1""",
             "star_schema": """
                WITH m AS (SELECT d.year_month, c.channel_group, sum(f.net_sales_usd) AS net_usd
                           FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key
                           JOIN dim_channel c ON c.channel_key = f.channel_key
                           JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                           WHERE sp.date_quality_flag = 'Normal' GROUP BY 1,2)
                SELECT year_month, channel_group, round(net_usd,2) AS net_usd,
                       round(100*(net_usd / nullif(lag(net_usd,12) OVER (PARTITION BY channel_group ORDER BY year_month),0) - 1),2) AS yoy_pct
                FROM m ORDER BY 2,1""",
             "star_optimized": """
                WITH m AS (SELECT year_month, channel_group, sum(net_usd) AS net_usd FROM agg_sales_daily_channel
                           WHERE date_quality_flag = 'Normal' GROUP BY 1,2)
                SELECT year_month, channel_group, round(net_usd,2) AS net_usd,
                       round(100*(net_usd / nullif(lag(net_usd,12) OVER (PARTITION BY channel_group ORDER BY year_month),0) - 1),2) AS yoy_pct
                FROM m ORDER BY 2,1"""}),
    dict(id="Q07", widget="Top 10 fast-moving medicines (by therapeutic class)", dashboard=DASH_VEL,
         question="Which medicines move fastest, and where do they rank inside their therapeutic class?",
         optimization="agg_medicine_velocity (63-row pre-ranked table)",
         sql={
             "oltp_staging": f"""
                WITH s AS (SELECT medicine_id, sum(quantity_sold_packs) AS packs,
                                  count(DISTINCT to_char(sales_date,'YYYY-MM')) AS months
                           FROM staging.stg_pharmacy_sales_data WHERE {PG_NORMAL} GROUP BY 1),
                     m AS (SELECT DISTINCT ON (medicine_id) medicine_id, generic_name, therapeutic_class, cold_chain_flag
                           FROM staging.stg_master_data ORDER BY medicine_id, effective_from_date DESC)
                SELECT m.generic_name, m.therapeutic_class, m.cold_chain_flag, round(s.packs::numeric / s.months, 1) AS packs_per_month,
                       rank() OVER (PARTITION BY m.therapeutic_class ORDER BY s.packs::numeric / s.months DESC) AS rank_in_class
                FROM s JOIN m USING (medicine_id) ORDER BY packs_per_month DESC, 1 LIMIT 10""",
             "star_schema": """
                WITH s AS (SELECT m.medicine_id, any_value(m.generic_name) AS generic_name,
                                  any_value(m.therapeutic_class) AS therapeutic_class,
                                  any_value(m.cold_chain_flag) AS cold_chain_flag,
                                  sum(f.quantity_sold_packs) AS packs, count(DISTINCT d.year_month) AS months
                           FROM fact_sales f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                           JOIN dim_date d ON d.date_key = f.sale_date_key
                           JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                           WHERE sp.date_quality_flag = 'Normal' GROUP BY 1)
                SELECT generic_name, therapeutic_class, cold_chain_flag, round(packs / months, 1) AS packs_per_month,
                       rank() OVER (PARTITION BY therapeutic_class ORDER BY packs / months DESC) AS rank_in_class
                FROM s ORDER BY packs_per_month DESC, 1 LIMIT 10""",
             "star_optimized": """
                SELECT generic_name, therapeutic_class, cold_chain_flag, round(packs_per_month, 1) AS packs_per_month,
                       rank_in_class FROM agg_medicine_velocity ORDER BY packs_per_month DESC, 1 LIMIT 10"""}),
    dict(id="Q08", widget="Slow-moving medicines & cold-chain items", dashboard=DASH_VEL,
         question="Which medicines (especially high-value cold-chain items) are the slowest movers?",
         optimization="agg_medicine_velocity (63-row pre-ranked table)",
         sql={
             "oltp_staging": f"""
                WITH s AS (SELECT medicine_id, sum(quantity_sold_packs) AS packs, sum(net_sales_amount_usd) AS net_usd,
                                  count(DISTINCT to_char(sales_date,'YYYY-MM')) AS months
                           FROM staging.stg_pharmacy_sales_data WHERE {PG_NORMAL} GROUP BY 1),
                     m AS (SELECT DISTINCT ON (medicine_id) medicine_id, generic_name, therapeutic_class, cold_chain_flag
                           FROM staging.stg_master_data ORDER BY medicine_id, effective_from_date DESC)
                SELECT m.generic_name, m.therapeutic_class, m.cold_chain_flag, round(s.packs::numeric / s.months, 1) AS packs_per_month,
                       round(s.net_usd::numeric, 2) AS net_usd
                FROM s JOIN m USING (medicine_id) ORDER BY packs_per_month ASC, 1 LIMIT 10""",
             "star_schema": """
                WITH s AS (SELECT m.medicine_id, any_value(m.generic_name) AS generic_name,
                                  any_value(m.therapeutic_class) AS therapeutic_class,
                                  any_value(m.cold_chain_flag) AS cold_chain_flag, sum(f.quantity_sold_packs) AS packs,
                                  sum(f.net_sales_usd) AS net_usd, count(DISTINCT d.year_month) AS months
                           FROM fact_sales f JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                           JOIN dim_date d ON d.date_key = f.sale_date_key
                           JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                           WHERE sp.date_quality_flag = 'Normal' GROUP BY 1)
                SELECT generic_name, therapeutic_class, cold_chain_flag, round(packs / months, 1) AS packs_per_month,
                       round(net_usd, 2) AS net_usd FROM s ORDER BY packs_per_month ASC, 1 LIMIT 10""",
             "star_optimized": """
                SELECT generic_name, therapeutic_class, cold_chain_flag, round(packs_per_month, 1) AS packs_per_month,
                       round(net_usd, 2) AS net_usd FROM agg_medicine_velocity ORDER BY packs_per_month ASC, 1 LIMIT 10"""}),
    dict(id="Q09", widget="Unified demand by region x channel (incl. hospital wards), last 12 months", dashboard=DASH_DEM,
         question="What is total medicine demand per region across ALL channels, including hospital ward consumption?",
         optimization="agg_unified_demand_monthly = pre-computed drill-across of both facts on conformed dimensions",
         sql={
             "oltp_staging": f"""
                WITH lim AS (SELECT to_char(max(sales_date) - interval '11 months','YYYY-MM') AS m0
                             FROM staging.stg_pharmacy_sales_data),
                u AS (
                  SELECT {region_case('pharmacy_state')} AS census_region, {PG_CHANNEL} AS channel_group,
                         quantity_sold_packs AS packs, to_char(sales_date,'YYYY-MM') AS ym
                  FROM staging.stg_pharmacy_sales_data WHERE {PG_NORMAL}
                  UNION ALL
                  SELECT {region_case('state')}, 'Hospital ward', consumed_quantity_packs, to_char(demand_month,'YYYY-MM')
                  FROM staging.stg_hospital_demand_data)
                SELECT census_region, channel_group, sum(packs) AS demand_packs
                FROM u, lim WHERE ym >= lim.m0 GROUP BY 1,2 ORDER BY 1,2""",
             "star_schema": """
                WITH lim AS (SELECT strftime(max(d.full_date) - INTERVAL 11 MONTH, '%Y-%m') AS m0
                             FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key),
                u AS (
                  SELECT g.census_region, c.channel_group, f.quantity_sold_packs AS packs, d.year_month AS ym
                  FROM fact_sales f JOIN dim_geography g ON g.geography_key = f.geography_key
                  JOIN dim_channel c ON c.channel_key = f.channel_key JOIN dim_date d ON d.date_key = f.sale_date_key
                  JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                  WHERE sp.date_quality_flag = 'Normal'
                  UNION ALL
                  SELECT g.census_region, c.channel_group, f.consumed_qty_packs, d.year_month
                  FROM fact_hospital_demand f JOIN dim_geography g ON g.geography_key = f.geography_key
                  JOIN dim_channel c ON c.channel_key = f.channel_key JOIN dim_date d ON d.date_key = f.demand_month_key)
                SELECT census_region, channel_group, sum(packs) AS demand_packs
                FROM u, lim WHERE ym >= lim.m0 GROUP BY 1,2 ORDER BY 1,2""",
             "star_optimized": """
                WITH lim AS (SELECT strftime(strptime(max(year_month) FILTER (WHERE channel_group <> 'Hospital ward'),
                                     '%Y-%m') - INTERVAL 11 MONTH, '%Y-%m') AS m0 FROM agg_unified_demand_monthly)
                SELECT census_region, channel_group, sum(demand_packs) AS demand_packs
                FROM agg_unified_demand_monthly, lim WHERE year_month >= lim.m0 GROUP BY 1,2 ORDER BY 1,2"""}),
    dict(id="Q10", widget="Regional x disease-category roll-up (ROLLUP)", dashboard=DASH_DEM,
         question="How is revenue distributed across regions and disease categories, with sub-totals?",
         optimization="agg_sales_monthly + GROUP BY ROLLUP on the aggregate",
         sql={
             "oltp_staging": f"""
                SELECT {region_case('s.pharmacy_state')} AS census_region, s.disease_category,
                       round(sum(s.net_sales_amount_usd)::numeric,2) AS net_usd, sum(s.quantity_sold_packs) AS packs
                FROM staging.stg_pharmacy_sales_data s WHERE {PG_NORMAL.replace('sales_date','s.sales_date').replace('pharmacy_delivery_date','s.pharmacy_delivery_date')}
                GROUP BY ROLLUP (1, 2) ORDER BY 1 NULLS LAST, 2 NULLS LAST""",
             "star_schema": """
                SELECT g.census_region, m.disease_category, round(sum(f.net_sales_usd),2) AS net_usd,
                       sum(f.quantity_sold_packs) AS packs
                FROM fact_sales f JOIN dim_geography g ON g.geography_key = f.geography_key
                JOIN dim_medicine m ON m.medicine_key = f.medicine_key
                JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                WHERE sp.date_quality_flag = 'Normal'
                GROUP BY ROLLUP (g.census_region, m.disease_category) ORDER BY 1 NULLS LAST, 2 NULLS LAST""",
             "star_optimized": """
                SELECT census_region, disease_category, round(sum(net_usd),2) AS net_usd, sum(qty_packs) AS packs
                FROM agg_sales_monthly WHERE date_quality_flag = 'Normal'
                GROUP BY ROLLUP (census_region, disease_category) ORDER BY 1 NULLS LAST, 2 NULLS LAST"""}),
    dict(id="Q11", widget="Promotion effectiveness (2025 onwards)", dashboard=DASH_CHAN,
         question="Do promotions lift basket size or just give away margin?",
         optimization="Re-clustered fact (ORDER BY sale_date_key) -> zone maps skip pre-2025 row groups. "
                      "Extra tier lake_parquet reads the hive-partitioned Parquet copy (partition pruning)",
         sql={
             "oltp_staging": """
                SELECT coalesce(promotion_code,'NO_PROMO') AS promotion_code, count(*) AS txns,
                       round(avg(quantity_sold_packs)::numeric,3) AS avg_packs_per_txn,
                       round(sum(discount_amount_usd)::numeric,2) AS discount_usd,
                       round(100*sum(gross_margin_usd)::numeric/nullif(sum(net_sales_amount_usd),0)::numeric,2) AS margin_pct
                FROM staging.stg_pharmacy_sales_data WHERE sales_date >= DATE '2025-01-01'
                GROUP BY 1 ORDER BY 1""",
             "star_schema": """
                SELECT p.promotion_code, count(*) AS txns, round(avg(f.quantity_sold_packs),3) AS avg_packs_per_txn,
                       round(sum(f.discount_usd),2) AS discount_usd,
                       round(100*sum(f.gross_margin_usd)/nullif(sum(f.net_sales_usd),0),2) AS margin_pct
                FROM fact_sales f JOIN dim_promotion p ON p.promotion_key = f.promotion_key
                WHERE f.sale_date_key >= 20250101 GROUP BY 1 ORDER BY 1""",
             "star_optimized": """
                SELECT p.promotion_code, count(*) AS txns, round(avg(f.quantity_sold_packs),3) AS avg_packs_per_txn,
                       round(sum(f.discount_usd),2) AS discount_usd,
                       round(100*sum(f.gross_margin_usd)/nullif(sum(f.net_sales_usd),0),2) AS margin_pct
                FROM fact_sales f JOIN dim_promotion p ON p.promotion_key = f.promotion_key
                WHERE f.sale_date_key >= 20250101 GROUP BY 1 ORDER BY 1""",
             "lake_parquet": f"""
                SELECT p.promotion_code, count(*) AS txns, round(avg(f.quantity_sold_packs),3) AS avg_packs_per_txn,
                       round(sum(f.discount_usd),2) AS discount_usd,
                       round(100*sum(f.gross_margin_usd)/nullif(sum(f.net_sales_usd),0),2) AS margin_pct
                FROM {PARQUET} f JOIN dim_promotion p ON p.promotion_key = f.promotion_key
                WHERE f.sale_year >= 2025 GROUP BY 1 ORDER BY 1"""}),
    dict(id="Q12", widget="E-commerce fulfilment: returned-to-sender rate by month", dashboard=DASH_CHAN,
         question="Is the e-commerce channel failing to deliver (returned-to-sender) and is it getting worse?",
         optimization="agg_sales_monthly carries pre-computed rts_txn / ecom_txn counters",
         sql={
             "oltp_staging": """
                SELECT to_char(sales_date,'YYYY-MM') AS year_month, count(*) AS ecom_orders,
                       count(*) FILTER (WHERE ecommerce_delivery_status = 'Returned to sender') AS returned_to_sender,
                       round(100.0*count(*) FILTER (WHERE ecommerce_delivery_status = 'Returned to sender')/count(*),2) AS rts_pct
                FROM staging.stg_pharmacy_sales_data WHERE sales_channel = 'Ecommerce' GROUP BY 1 ORDER BY 1""",
             "star_schema": """
                SELECT d.year_month, count(*) AS ecom_orders,
                       count(*) FILTER (WHERE sp.ecommerce_delivery_status = 'Returned to sender') AS returned_to_sender,
                       round(100.0*count(*) FILTER (WHERE sp.ecommerce_delivery_status = 'Returned to sender')/count(*),2) AS rts_pct
                FROM fact_sales f JOIN dim_date d ON d.date_key = f.sale_date_key
                JOIN dim_channel c ON c.channel_key = f.channel_key
                JOIN dim_sales_profile sp ON sp.sales_profile_key = f.sales_profile_key
                WHERE c.channel_group = 'E-commerce' GROUP BY 1 ORDER BY 1""",
             "star_optimized": """
                SELECT year_month, sum(txn_count) AS ecom_orders, sum(rts_txn) AS returned_to_sender,
                       round(100.0*sum(rts_txn)/sum(txn_count),2) AS rts_pct
                FROM agg_sales_monthly WHERE channel_group = 'E-commerce' GROUP BY 1 ORDER BY 1"""}),
    dict(id="Q13", widget="SCD2 price-change impact (medicine versions)", dashboard=DASH_CHAN,
         question="How did volume and revenue react after a medicine's price changed (SCD Type-2 versions)?",
         optimization="Point lookup on the small SCD2 dimension first, then fact filtered by medicine_key",
         sql={
             "oltp_staging": """
                WITH r AS (SELECT medicine_id, recommended_retail_price_usd AS rrp, effective_from_date AS eff,
                                  lag(recommended_retail_price_usd) OVER (PARTITION BY medicine_id ORDER BY effective_from_date) AS prev_rrp,
                                  first_value(generic_name) OVER (PARTITION BY medicine_id ORDER BY effective_from_date DESC) AS generic_name
                           FROM staging.stg_master_data),
                     chg AS (SELECT * FROM r WHERE prev_rrp IS NULL OR prev_rrp <> rrp),   -- price change = new version
                     v AS (SELECT medicine_id, generic_name, rrp, eff AS eff_from,
                                  coalesce(lead(eff) OVER (PARTITION BY medicine_id ORDER BY eff) - 1, DATE '9999-12-31') AS eff_to
                           FROM chg WHERE medicine_id IN (SELECT medicine_id FROM chg GROUP BY 1 HAVING count(*) > 1))
                SELECT v.generic_name, v.rrp, v.eff_from, count(s.sales_transaction_id) AS txns,
                       coalesce(sum(s.quantity_sold_packs),0) AS packs, round(coalesce(sum(s.net_sales_amount_usd),0)::numeric,2) AS net_usd
                FROM v LEFT JOIN staging.stg_pharmacy_sales_data s ON s.medicine_id = v.medicine_id
                     AND s.sales_date BETWEEN v.eff_from AND v.eff_to AND s.sales_date >= DATE '2025-07-01'
                GROUP BY 1,2,3 ORDER BY 1,3""",
             "star_schema": """
                SELECT m.generic_name, m.recommended_retail_price_usd AS rrp, m.effective_from AS eff_from,
                       count(f.sales_transaction_id) AS txns, coalesce(sum(f.quantity_sold_packs),0) AS packs,
                       round(coalesce(sum(f.net_sales_usd),0),2) AS net_usd
                FROM dim_medicine m LEFT JOIN fact_sales f ON f.medicine_key = m.medicine_key AND f.sale_date_key >= 20250701
                WHERE m.medicine_id IN (SELECT medicine_id FROM dim_medicine GROUP BY 1 HAVING count(*) > 1)
                GROUP BY 1,2,3 ORDER BY 1,3""",
             "star_optimized": """
                WITH v AS (SELECT medicine_key, generic_name, recommended_retail_price_usd AS rrp, effective_from
                           FROM dim_medicine WHERE medicine_id IN (SELECT medicine_id FROM dim_medicine
                                                                   WHERE version_num > 1))
                SELECT v.generic_name, v.rrp, v.effective_from AS eff_from, count(f.sales_transaction_id) AS txns,
                       coalesce(sum(f.quantity_sold_packs),0) AS packs, round(coalesce(sum(f.net_sales_usd),0),2) AS net_usd
                FROM v LEFT JOIN fact_sales f ON f.medicine_key = v.medicine_key AND f.sale_date_key >= 20250701
                GROUP BY 1,2,3 ORDER BY 1,3"""}),
]

# Simulated ad-hoc analyst queries (2 of 6 contain deliberate mistakes) -> exercises the query-error-rate metric
ADHOC = [
    ("ADH01", "duckdb", "SELECT count(*) FROM fact_sales WHERE sale_date_key >= 20260101"),
    ("ADH02", "duckdb", "SELECT medicine_id, sum(qty_packs) FROM agg_sales_monthly GROUP BY 1 ORDER BY 2 DESC LIMIT 5"),
    ("ADH03", "duckdb", "SELECT hospital_nam, sum(consumed_packs) FROM agg_hospital_monthly GROUP BY 1"),   # typo
    ("ADH04", "postgres", "SELECT count(*) FROM staging.stg_pharmacy_sales_data WHERE _ingest_mode = 'stream'"),
    ("ADH05", "duckdb", "SELECT * FROM fact_sales_2027"),                                                     # missing table
    ("ADH06", "duckdb", "SELECT channel_group, sum(net_usd) FROM agg_sales_monthly GROUP BY 1"),
]

# The OLTP tier reads the committed-snapshot views staging.c_stg_* (rows <= the last committed transform watermark),
# so it describes exactly the data that is in the mart even while new data is still streaming into staging.
for _q in QUERIES:
    _q["sql"]["oltp_staging"] = _q["sql"]["oltp_staging"].replace("staging.stg_", "staging.c_stg_")

BY_ID = {q["id"]: q for q in QUERIES}
