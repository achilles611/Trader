#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /absolute/path/to/cycle_bundle.json" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config/global.yaml}"

cd "$ROOT_DIR"
exec "$PYTHON_BIN" -m src.orchestrator --config "$CONFIG_PATH" replay-analysis --bundle "$1"
