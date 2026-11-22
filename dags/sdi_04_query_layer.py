"""DAG sdi_04_query_layer - Analytical Query Layer & Query Optimisation.

apply_optimizations (re-cluster fact, ART indexes, partitioned Parquet) -> benchmark (13 business queries x tiers:
Postgres staging vs star schema vs optimised star, 3 repeats, result-equivalence check) -> simulated BI dashboard
refresh -> simulated ad-hoc analyst queries -> storage metrics. Everything is timed into ops.query_log."""
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from sdi_common import DEFAULT_ARGS, START, TAGS, run_id_of, refresh_tag_of


@dag(dag_id="sdi_04_query_layer", start_date=START, schedule="0 6 * * *", catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["query", "optimization"], max_active_runs=1, doc_md=__doc__)
def sdi_04_query_layer():

    @task.short_circuit         # skips the run (no failure) while the mart has not been built yet
    def apply_optimizations():
        from sdi.query.runner import optimize_mart
        return optimize_mart(run_id_of(get_current_context()))

    @task
    def run_benchmark():
        from sdi.query.runner import benchmark
        ctx = get_current_context()
        r = benchmark(run_id_of(ctx), refresh_tag_of(ctx))
        bad = [f"{k[0]}/{k[1]}" for k, ok in r["equivalence"].items() if not ok]
        if bad:
            raise RuntimeError(f"tiers disagree: {bad}")
        return {"executions": r["executions"]}

    @task
    def dashboard_refresh_simulation():
        from sdi.query.runner import dashboard_refresh
        ctx = get_current_context()
        return dashboard_refresh(run_id_of(ctx), refresh_tag_of(ctx))

    @task
    def adhoc_query_simulation():
        from sdi.query.runner import adhoc_simulation
        ctx = get_current_context()
        return adhoc_simulation(run_id_of(ctx), refresh_tag_of(ctx))

    trigger_storage = TriggerDagRunOperator(task_id="trigger_storage_metrics", trigger_dag_id="sdi_05_storage_metrics",
                                            conf={"refresh_tag": "{{ dag_run.conf.get('refresh_tag') if dag_run.conf else None }}"},
                                            wait_for_completion=False)

    apply_optimizations() >> run_benchmark() >> dashboard_refresh_simulation() >> adhoc_query_simulation() >> trigger_storage


sdi_04_query_layer()
