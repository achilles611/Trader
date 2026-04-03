#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

git fetch --prune origin
git checkout main
git reset --hard origin/main
git submodule update --init --recursive

if command -v systemctl >/dev/null 2>&1; then
  sudo systemctl restart trader-swarm.service || true
fi
