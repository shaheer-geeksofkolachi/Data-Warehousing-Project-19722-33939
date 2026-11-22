"""DAG sdi_01_batch_ingestion - Batch Data Ingestion Layer (Excel -> Postgres staging / ODS).

Triggered immediately by the file-watcher (via the ingestion API -> Airflow REST API) and, as a safety net, polls
ops.file_registry every 2 minutes. Master data is loaded first (referential checks need it); the other files are
loaded with dynamic task mapping (one mapped task instance per file). Every sheet's duration / rows / rejects /
status lands in ops.batch_load_log (Grafana: Batch Ingestion)."""
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from sdi_common import DEFAULT_ARGS, START, TAGS, run_id_of


@dag(dag_id="sdi_01_batch_ingestion", start_date=START, schedule="*/2 * * * *", catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["ingestion", "batch"], max_active_runs=1, doc_md=__doc__)
def sdi_01_batch_ingestion():

    @task.short_circuit
    def discover_pending_files() -> list:
        from sdi.ingestion.batch_loader import pending_files
        return [str(p) for p in pending_files()]

    @task
    def load_master_first(files: list) -> list:
        from sdi.ingestion.batch_loader import process_file
        ctx = get_current_context()
        logs = []
        for f in files:
            if "01_master_data" in f or "Source_Data" in f:   # full workbook contains master too
                logs += process_file(f, run_id_of(ctx))
        return [f for f in files if not ("01_master_data" in f or "Source_Data" in f)]

    @task(max_active_tis_per_dag=2)
    def load_file(path: str) -> dict:
        from sdi.ingestion.batch_loader import process_file
        logs = process_file(path, run_id_of(get_current_context()))
        failed = [l for l in logs if l["status"] != "SUCCESS"]
        if failed:
            raise RuntimeError(f"{path}: {failed[0]['error_message']}")
        return {"file": path, "rows_loaded": sum(l["rows_loaded"] for l in logs),
                "rows_rejected": sum(l["rows_rejected"] for l in logs)}

    @task(trigger_rule="none_failed")
    def summarize(results: list) -> dict:
        results = list(results or [])
        return {"files": len(results), "rows_loaded": sum(r["rows_loaded"] for r in results),
                "rows_rejected": sum(r["rows_rejected"] for r in results)}

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform", trigger_dag_id="sdi_03_transform",
        conf={"refresh_tag": "{{ dag_run.conf.get('refresh_tag') if dag_run.conf else None }}"},
        wait_for_completion=False, trigger_rule="none_failed")

    files = discover_pending_files()
    rest = load_master_first(files)
    summarize(load_file.expand(path=rest)) >> trigger_transform


sdi_01_batch_ingestion()
