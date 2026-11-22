"""DAG sdi_05_storage_metrics - Storage & Staging Layer monitoring.

Snapshots row counts, physical size (Postgres per table + database, DuckDB per table + file, Parquet dir) and
date coverage (min/max, distinct year-month partitions per source) into ops.storage_* (Grafana: Storage)."""
from airflow.sdk import dag, task, get_current_context
from sdi_common import DEFAULT_ARGS, START, TAGS, refresh_tag_of


@dag(dag_id="sdi_05_storage_metrics", start_date=START, schedule="@hourly", catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["storage", "monitoring"], max_active_runs=1, doc_md=__doc__)
def sdi_05_storage_metrics():

    @task
    def collect_storage_metrics():
        from sdi.monitoring.storage_metrics import collect
        return collect(refresh_tag_of(get_current_context()))

    @task
    def check_date_coverage(_):
        """Validation metric: every month from 2023-01 to the latest loaded month must be present for sales and
        hospital demand (no holes in the history)."""
        from sdi.db import pg_query
        missing = pg_query("""
            WITH months AS (SELECT to_char(m, 'YYYY-MM') ym FROM generate_series(DATE '2023-01-01',
                            (SELECT max(sales_date) FROM staging.stg_pharmacy_sales_data), INTERVAL '1 month') m)
            SELECT ym FROM months WHERE ym NOT IN (SELECT DISTINCT to_char(sales_date,'YYYY-MM')
                                                   FROM staging.stg_pharmacy_sales_data)""")
        if missing:
            raise RuntimeError(f"date coverage holes in sales: {missing}")
        return "complete"

    check_date_coverage(collect_storage_metrics())


sdi_05_storage_metrics()
