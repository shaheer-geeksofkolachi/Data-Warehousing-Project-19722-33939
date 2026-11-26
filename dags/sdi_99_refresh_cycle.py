"""DAG sdi_99_refresh_cycle - one-click end-to-end refresh (used in the demo).

generator -> batch ingestion -> stream ingestion -> transform -> query layer -> BI refresh (Superset), each step waiting
for the previous DAG run to finish. Choose refresh_tag BASE (initial historical load from the provided workbook),
R1 or R2."""
from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from sdi_common import DEFAULT_ARGS, START, TAGS


@dag(dag_id="sdi_99_refresh_cycle", start_date=START, schedule=None, catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["orchestration"], max_active_runs=1,
     params={"refresh_tag": Param("R1", enum=["BASE", "R1", "R2"])}, doc_md=__doc__)
def sdi_99_refresh_cycle():

    @task.branch
    def base_or_refresh():
        tag = get_current_context()["params"]["refresh_tag"]
        return "stage_base_workbook" if tag == "BASE" else "generate"

    @task
    def stage_base_workbook():
        import shutil
        from sdi.config import SOURCE_DIR, LANDING_DIR
        for f in SOURCE_DIR.glob("BASE_*.xlsx"):
            shutil.copy2(f, LANDING_DIR / f.name)

    conf = {"refresh_tag": "{{ params.refresh_tag }}"}
    gen = TriggerDagRunOperator(task_id="generate", trigger_dag_id="sdi_00_data_generator", conf=conf,
                                wait_for_completion=True, poke_interval=10)
    batch = TriggerDagRunOperator(task_id="batch_ingestion", trigger_dag_id="sdi_01_batch_ingestion", conf=conf,
                                  wait_for_completion=True, poke_interval=10, trigger_rule="none_failed_min_one_success")
    stream = TriggerDagRunOperator(task_id="stream_ingestion", trigger_dag_id="sdi_02_stream_ingestion", conf=conf,
                                   wait_for_completion=True, poke_interval=10)
    transform = TriggerDagRunOperator(task_id="transform", trigger_dag_id="sdi_03_transform", conf=conf,
                                      wait_for_completion=True, poke_interval=10)
    query = TriggerDagRunOperator(task_id="query_layer", trigger_dag_id="sdi_04_query_layer", conf=conf,
                                  wait_for_completion=True, poke_interval=10)
    bi = TriggerDagRunOperator(task_id="bi_refresh", trigger_dag_id="sdi_06_bi_refresh", conf=conf,
                               wait_for_completion=True, poke_interval=10)

    branch = base_or_refresh()
    stage = stage_base_workbook()
    branch >> [stage, gen]
    [stage, gen] >> batch          # batch runs after whichever branch was taken
    batch >> stream >> transform >> query >> bi


sdi_99_refresh_cycle()
