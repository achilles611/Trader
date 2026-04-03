#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
CONFIG_PATH="${CONFIG_PATH:-$ROOT_DIR/config/global.yaml}"

cd "$ROOT_DIR"
exec "$PYTHON_BIN" -m src.orchestrator --config "$CONFIG_PATH" run-cycle
