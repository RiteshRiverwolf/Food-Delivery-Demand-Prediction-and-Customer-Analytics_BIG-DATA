#!/usr/bin/env bash
# Runs the three MRJob jobs locally on the full data and records run times.
# Usage: bash src/mapreduce/run_local.sh [raw_dir]
set -euo pipefail
RAW=${1:-data/raw}
OUT=outputs/mapreduce
PY=.venv/bin/python
mkdir -p "$OUT"
: > "$OUT/timings_local.txt"

run() {  # name, then the job command
  local name=$1; shift
  local start=$(date +%s)
  "$@" > "$OUT/$name.csv"
  echo "$name: $(( $(date +%s) - start ))s" | tee -a "$OUT/timings_local.txt"
}

run orders_by_time $PY src/mapreduce/mr_orders_by_time.py -r local "$RAW/orders.csv"
run top_products $PY src/mapreduce/mr_top_products.py -r local --products "$RAW/products.csv" "$RAW/order_products__prior.csv"
run department_demand $PY src/mapreduce/mr_department_demand.py -r local \
  --products "$RAW/products.csv" --departments "$RAW/departments.csv" "$RAW/order_products__prior.csv"
