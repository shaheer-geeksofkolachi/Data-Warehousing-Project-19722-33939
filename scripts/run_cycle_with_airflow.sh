#!/usr/bin/env bash
# Runs one complete refresh cycle by executing every DAG with `airflow dags test` (no scheduler needed).
# In the Docker stack the scheduler does this automatically (or trigger sdi_99_refresh_cycle in the UI).
#   usage: scripts/run_cycle_with_airflow.sh BASE|R1|R2
set -euo pipefail
TAG=${1:?refresh tag BASE|R1|R2}
CONF="{\"refresh_tag\": \"$TAG\"}"
AF=${AIRFLOW_BIN:-airflow}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
run() { echo ">>> $1 ($TAG)"; $AF dags test "$1" -c "$CONF" > "$ROOT/data/work/airflow_${TAG}_$1.log" 2>&1 \
          && echo "    $(grep -cE 'Marking task as SUCCESS|state=success|success' "$ROOT/data/work/airflow_${TAG}_$1.log") success markers" \
          || { echo "    FAILED - see data/work/airflow_${TAG}_$1.log"; tail -30 "$ROOT/data/work/airflow_${TAG}_$1.log"; exit 1; }; }

if [ "$TAG" = "BASE" ]; then
  cp "$ROOT"/data/source/BASE_*.xlsx "$ROOT/data/landing/"     # file-watcher picks it up
else
  run sdi_00_data_generator                                      # publishes to landing/ + stream_queue/
fi
sleep 8                                                          # give the watcher time to register the files
run sdi_01_batch_ingestion
[ "$TAG" = "BASE" ] || run sdi_02_stream_ingestion
run sdi_03_transform
run sdi_04_query_layer
run sdi_05_storage_metrics
run sdi_06_bi_refresh
echo "cycle $TAG complete"
