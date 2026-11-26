"""DAG sdi_03_transform - Transformation & Dimensional Modelling (Postgres staging -> DuckDB SDI star schema).

init_mart -> extract_delta (watermark) -> load_dimensions (SCD1/SCD2, junk, role-playing date)
         -> load_fact_sales -> load_fact_hospital_demand -> build_aggregates (materialised summaries)
         -> reconcile_and_commit (staging vs mart; watermark advances only on success) -> trigger query layer.
DuckDB allows one writer at a time, so mart-writing tasks are chained (not parallel)."""
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from sdi_common import DEFAULT_ARGS, START, TAGS, run_id_of, refresh_tag_of


@dag(dag_id="sdi_03_transform", start_date=START, schedule="@hourly", catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["transform", "dimensional-model"], max_active_runs=1, doc_md=__doc__)
def sdi_03_transform():

    @task
    def init_mart():
        from sdi.warehouse.transform import init_mart as f
        f()

    @task.short_circuit
    def extract_delta() -> bool:
        from sdi.warehouse.transform import extract_delta as f
        ctx = get_current_context()
        m = f(run_id_of(ctx), refresh_tag_of(ctx))
        return sum(t["rows"] for t in m["tables"].values()) > 0     # nothing new -> skip the rest

    @task
    def load_dimensions():
        from sdi.warehouse.transform import load_dimensions as f
        return f(run_id_of(get_current_context()))

    @task
    def load_fact_sales():
        from sdi.warehouse.transform import load_fact_sales as f
        return f(run_id_of(get_current_context()))

    @task
    def load_fact_hospital_demand():
        from sdi.warehouse.transform import load_fact_hospital_demand as f
        return f(run_id_of(get_current_context()))

    @task
    def build_aggregates():
        from sdi.warehouse.transform import build_aggregates as f
        return f(run_id_of(get_current_context()))

    @task
    def reconcile_and_commit():
        from sdi.warehouse.transform import reconcile_and_commit as f
        return f(run_id_of(get_current_context()))

    trigger_query = TriggerDagRunOperator(task_id="trigger_query_layer", trigger_dag_id="sdi_04_query_layer",
                                          wait_for_completion=False)
    trigger_bi = TriggerDagRunOperator(task_id="trigger_bi_refresh", trigger_dag_id="sdi_06_bi_refresh",
                                       wait_for_completion=False)

    (init_mart() >> extract_delta() >> load_dimensions() >> load_fact_sales() >> load_fact_hospital_demand()
     >> build_aggregates() >> reconcile_and_commit() >> [trigger_query, trigger_bi])


sdi_03_transform()
