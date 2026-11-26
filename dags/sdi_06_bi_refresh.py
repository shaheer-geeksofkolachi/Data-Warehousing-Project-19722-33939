"""DAG sdi_06_bi_refresh - Business Intelligence layer refresh (Apache Superset).

publish_bi_mart      DuckDB gold mart -> Postgres serving schema sdi_bi (built in sdi_bi_new, swapped atomically)
snapshot_bi_kpis     headline KPIs of every dashboard -> ops.bi_kpi_snapshot (before/after evidence per refresh)
sync_superset        idempotent dashboards-as-code: database, datasets (column refresh), charts, 4 dashboards
warm_bi_dashboards   fetch every dashboard chart through Superset (live query, no cache), timed -> ops.query_log
                     (caller = bi_dashboard): the 'BI component fetches data' counter of the Query dashboard
Triggered by sdi_03_transform after every successful reconcile, and by sdi_99_refresh_cycle."""
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.sensors.python import PythonSensor
from sdi_common import DEFAULT_ARGS, START, TAGS, run_id_of, refresh_tag_of


def _superset_up():
    import requests
    from sdi.bi.superset import SUPERSET_URL
    try:
        return requests.get(f"{SUPERSET_URL}/health", timeout=5).status_code == 200
    except Exception:
        return False


@dag(dag_id="sdi_06_bi_refresh", start_date=START, schedule="@hourly", catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["bi", "superset"], max_active_runs=1, doc_md=__doc__)
def sdi_06_bi_refresh():

    @task.short_circuit
    def publish_bi_mart():
        from sdi.bi.publish import publish
        from sdi.config import DUCKDB_PATH
        if not DUCKDB_PATH.exists():
            return None                      # fresh stack: nothing to publish yet -> skip (no failure)
        ctx = get_current_context()
        return publish(run_id_of(ctx), refresh_tag_of(ctx))

    @task
    def snapshot_bi_kpis(pub: dict):
        from sdi.bi.publish import snapshot_kpis
        return snapshot_kpis(run_id_of(get_current_context()), pub["refresh_tag"])

    wait_for_superset = PythonSensor(task_id="wait_for_superset", python_callable=_superset_up, poke_interval=15,
                                     timeout=600, mode="reschedule")

    @task
    def sync_superset():
        from sdi.bi.superset import provision_all
        return provision_all()

    @task
    def warm_bi_dashboards(pub: dict):
        from sdi.bi.superset import warm
        r = warm(run_id_of(get_current_context()), pub["refresh_tag"])
        if r["failed"]:
            raise RuntimeError(f"BI charts failed: {r['failed']}")
        return r

    pub = publish_bi_mart()
    kpis = snapshot_bi_kpis(pub)
    synced = sync_superset()
    kpis >> wait_for_superset >> synced >> warm_bi_dashboards(pub)


sdi_06_bi_refresh()
