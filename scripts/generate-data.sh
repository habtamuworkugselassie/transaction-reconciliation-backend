#!/usr/bin/env bash
# Generates an NDJSON test file. Usage: scripts/generate-data.sh --records=500000 [--out=path] [--invalid-rate=0.02]
set -euo pipefail
cd "$(dirname "$0")/.."
exec java scripts/GenerateData.java "$@"
