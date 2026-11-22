"""DAG sdi_00_data_generator - Data Oversampling Layer.

Synthesises one new month of data for all 7 source files (refresh R1 = Jan-2026, R2 = Feb-2026), validates the
output, records statistics in ops.generator_log, then publishes: 6 batch files -> landing/ (picked up by the
file-watcher) and the sales file -> stream_queue/ (replayed through the streaming API)."""
import json
from airflow.sdk import dag, task, Param, get_current_context
from sdi_common import DEFAULT_ARGS, START, TAGS, refresh_tag_of


@dag(dag_id="sdi_00_data_generator", start_date=START, schedule=None, catchup=False, default_args=DEFAULT_ARGS,
     tags=TAGS + ["oversampling"], max_active_runs=1,
     params={"refresh_tag": Param("R1", enum=["R1", "R2"], description="Refresh to generate")},
     doc_md=__doc__)
def sdi_00_data_generator():

    @task
    def generate_all_sources() -> list:
        from sdi.generator.oversample import Generator
        tag = refresh_tag_of(get_current_context(), "R1")
        return Generator(tag).run()

    @task
    def validate_generated(stats: list) -> list:
        from pathlib import Path
        for s in stats:
            assert Path(s["output_file"]).exists(), f"missing {s['output_file']}"
            assert s["rows_generated"] > 0, f"empty output for {s['source_name']}"
        return stats

    @task
    def log_generator_stats(stats: list) -> int:
        from sdi.db import pg_conn
        with pg_conn() as c, c.cursor() as cur:
            cur.executemany("""INSERT INTO ops.generator_log (refresh_tag, source_name, target_month, rows_generated,
                               dirty_rows, output_file) VALUES (%(refresh_tag)s, %(source_name)s, %(target_month)s,
                               %(rows_generated)s, %(dirty_rows)s, %(output_file)s)""", stats)
        return sum(s["rows_generated"] for s in stats)

    @task
    def publish_to_landing_and_stream_queue(_total: int) -> list:
        from sdi.generator.oversample import publish
        return publish(refresh_tag_of(get_current_context(), "R1"))

    stats = validate_generated(generate_all_sources())
    publish_to_landing_and_stream_queue(log_generator_stats(stats))


sdi_00_data_generator()
