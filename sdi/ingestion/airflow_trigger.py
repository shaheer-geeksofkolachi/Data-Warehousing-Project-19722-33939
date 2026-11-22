"""Optional: trigger an Airflow DAG run through the Airflow 3 REST API (v2).

The batch DAG ALSO polls ops.file_registry every 2 minutes, so ingestion still happens if this call fails
(e.g. Airflow API not reachable) - the trigger only makes the reaction to a new file immediate."""
import datetime as dt
import requests
from ..config import AIRFLOW_API_URL, AIRFLOW_USER, AIRFLOW_PASSWORD


def _token():
    r = requests.post(f"{AIRFLOW_API_URL}/auth/token", json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
                      timeout=5)
    r.raise_for_status()
    return r.json()["access_token"]


def trigger_dag(dag_id: str, conf: dict = None):
    if not AIRFLOW_API_URL:
        return False
    try:
        tok = _token()
        r = requests.post(f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns",
                          headers={"Authorization": f"Bearer {tok}"},
                          json={"logical_date": dt.datetime.now(dt.timezone.utc).isoformat(), "conf": conf or {}},
                          timeout=5)
        return r.status_code in (200, 201, 409)
    except Exception:
        return False
