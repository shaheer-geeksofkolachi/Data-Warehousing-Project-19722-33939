"""Shared settings for all SDI DAGs: default args (retries, timeouts), task-outcome callbacks, project import path."""
import os
import sys
import datetime as dt
from pathlib import Path

PROJECT_ROOT = os.getenv("SDI_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

START = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
TAGS = ["sdi", "phase1"]


def _log_task(context, state):
    try:
        from sdi.db import pg_conn
        ti = context["ti"]
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        start = getattr(ti, "start_date", None)
        end = dt.datetime.now(dt.timezone.utc)
        with pg_conn() as c, c.cursor() as cur:
            cur.execute("""INSERT INTO ops.pipeline_task_log (dag_id, task_id, run_id, try_number, state, start_ts,
                           end_ts, duration_s, refresh_tag) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (ti.dag_id, ti.task_id, ti.run_id, getattr(ti, "try_number", None), state, start, end,
                         (end - start).total_seconds() if start else None, conf.get("refresh_tag")))
    except Exception as e:  # monitoring must never break the pipeline
        print(f"[sdi] task log failed: {e}")


def on_success(context):
    _log_task(context, "success")


def on_failure(context):
    _log_task(context, "failed")


DEFAULT_ARGS = {
    "owner": "sdi-team",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=1),
    "execution_timeout": dt.timedelta(minutes=30),
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}


def run_id_of(context) -> str:
    return context["dag_run"].run_id if context.get("dag_run") else f"manual-{dt.datetime.now():%Y%m%dT%H%M%S}"


def refresh_tag_of(context, default=None):
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    params = context.get("params") or {}
    tag = conf.get("refresh_tag") or params.get("refresh_tag")
    return tag if tag not in (None, "", "None") else default   # Jinja renders a missing tag as the string "None"
