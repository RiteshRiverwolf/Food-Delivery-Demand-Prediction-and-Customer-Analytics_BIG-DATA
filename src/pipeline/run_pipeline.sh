#!/usr/bin/env bash
# Runs the full medallion pipeline: ingest -> bronze -> silver -> processing (gold) -> charts.
# Usage: bash src/pipeline/run_pipeline.sh   (from the project root)
set -euo pipefail
export SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PY="$ROOT/.venv/bin/python"
cd "$ROOT/src/pipeline"
for stage in 01_ingest 02_bronze 03_silver 04_processing; do
  echo "== $stage"
  "$PY" "$stage.py" --base "$ROOT/data" --reports "$ROOT/outputs/pipeline"
done
echo "== 05_charts"
"$PY" 05_charts.py --reports "$ROOT/outputs/pipeline"
