#!/usr/bin/env bash
# Repeatable benchmark. Requires a running stack (docker compose up, or the jar + postgres/kafka/redis).
#
#   scripts/benchmark.sh [records=500000] [base-url=http://localhost:8080] [metrics-url=http://localhost:8081/metrics]
#
# Generates data/transactions-<records>.ndjson if missing, uploads it, measures throughput,
# API latency during the import and peak resource usage, and writes benchmark-results/*.md.
set -euo pipefail
cd "$(dirname "$0")/.."
RECORDS="${1:-500000}"
BASE_URL="${2:-http://localhost:8080}"
METRICS_URL="${3:-http://localhost:8081/metrics}"
FILE="data/transactions-${RECORDS}.ndjson"
if [ ! -f "$FILE" ]; then
  java scripts/GenerateData.java --records="$RECORDS" --out="$FILE" --prefix="bench-$(date +%s)"
fi
exec java scripts/Benchmark.java --file="$FILE" --base-url="$BASE_URL" --metrics-url="$METRICS_URL"
