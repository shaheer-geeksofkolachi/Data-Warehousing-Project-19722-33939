"""DAG sdi_02_stream_ingestion - Streaming Data Ingestion Layer (sales events -> API -> Postgres staging).

Every 5 minutes (or when triggered by the generator/orchestrator) any sales increment waiting in stream_queue/ is
replayed as a live event stream: micro-batches of 50 events at a target rate (default 400 events/s) are POSTed to
the ingestion API, which validates each event, commits valid ones to staging and counts malformed/duplicate ones.
Per-second throughput, end-to-end latency and malformed counts land in ops.stream_* (Grafana: Streaming)."""
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor
from sdi_common import DEFAULT_ARGS, START, TAGS


def _api_healthy():
    import requests
    from sdi.config import INGEST_API_URL
    try:
        return requests.get(f"{INGEST_API_URL}/health", timeout=5).status_code == 200
    except Exception:
        return False


@dag(dag_id="sdi_02_stream_ingestion", start_date=START, schedule="*/5 * * * *", catchup=False,
     default_args=DEFAULT_ARGS, tags=TAGS + ["ingestion", "streaming"], max_active_runs=1, doc_md=__doc__)
def sdi_02_stream_ingestion():

    @task.short_circuit
    def check_stream_queue() -> list:
        from sdi.config import STREAM_QUEUE_DIR
        return [str(p) for p in sorted(STREAM_QUEUE_DIR.glob("*.xlsx"))]

    wait_for_api = PythonSensor(task_id="wait_for_ingestion_api", python_callable=_api_healthy, poke_interval=10,
                                timeout=300, mode="reschedule")

    @task
    def stream_sales_events(files: list) -> list:
        from sdi.ingestion.stream_producer import stream_file
        return [stream_file(f) for f in files]

    @task
    def validate_session(sessions: list) -> dict:
        for s in sessions:
            loss = s["events_sent"] - s["ingested"] - s["malformed"]
            # every event must be accounted for: ingested + malformed (+ duplicates rejected by the PK)
            assert loss >= 0, f"more events ingested than sent in {s['session_id']}"
        return {"sessions": len(sessions), "ingested": sum(s["ingested"] for s in sessions)}

    trigger_transform = TriggerDagRunOperator(task_id="trigger_transform", trigger_dag_id="sdi_03_transform",
                                              wait_for_completion=False)

    files = check_stream_queue()
    sessions = stream_sales_events(files)
    files >> wait_for_api >> sessions
    validate_session(sessions) >> trigger_transform


sdi_02_stream_ingestion()
