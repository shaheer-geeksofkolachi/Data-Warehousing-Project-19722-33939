"""Python file-watcher (watchdog) on the landing zone.

When a new .xlsx/.csv appears it waits until the file size is stable (fully written), then registers it with the
ingestion API (POST /batch/files), which records it in ops.file_registry and triggers the Airflow batch DAG.
If the API is down it registers the file directly in Postgres so nothing is lost.

Run:  python -m sdi.ingestion.watcher
"""
import time
import logging
from pathlib import Path

import requests
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler

from ..config import LANDING_DIR, INGEST_API_URL
from .batch_loader import register_file

log = logging.getLogger("sdi.watcher")
VALID = (".xlsx", ".csv")


def wait_until_stable(p: Path, checks=3, interval=1.0):
    last = -1
    stable = 0
    while stable < checks:
        size = p.stat().st_size if p.exists() else -1
        stable = stable + 1 if size == last and size > 0 else 0
        last = size
        time.sleep(interval)


def announce(p: Path):
    try:
        r = requests.post(f"{INGEST_API_URL}/batch/files", json={"path": str(p)}, timeout=10)
        log.info("registered via API %s -> %s", p.name, r.json())
    except Exception as e:
        log.warning("API unavailable (%s) - registering directly in ops.file_registry", e)
        register_file(p, detected_by="watcher")


class LandingHandler(FileSystemEventHandler):
    def _handle(self, path):
        p = Path(path)
        if p.suffix.lower() in VALID and not p.name.startswith(("~$", ".")):
            wait_until_stable(p)
            announce(p)

    def on_created(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_moved(self, event):      # atomic rename *.tmp -> *.xlsx used by the generator
        if not event.is_directory:
            self._handle(event.dest_path)


def main(polling=False):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    obs = (PollingObserver if polling else Observer)()
    obs.schedule(LandingHandler(), str(LANDING_DIR), recursive=True)
    obs.start()
    log.info("watching %s", LANDING_DIR)
    for p in LANDING_DIR.rglob("*"):          # files dropped while the watcher was down
        if p.is_file() and p.suffix.lower() in VALID:
            announce(p)
    try:
        while True:
            time.sleep(1)
    finally:
        obs.stop()
        obs.join()


if __name__ == "__main__":
    import os
    main(polling=os.getenv("SDI_WATCHER_POLLING", "0") == "1")
