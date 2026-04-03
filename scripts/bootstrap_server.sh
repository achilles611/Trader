#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/opt/trader}"
APP_USER="${APP_USER:-$(id -un)}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip sqlite3 rsync

sudo mkdir -p "$APP_DIR"
sudo rsync -a --delete --exclude '.git' --exclude '.venv' "$REPO_DIR"/ "$APP_DIR"/
sudo chown -R "$APP_USER":"$APP_USER" "$APP_DIR"

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
chmod +x "$APP_DIR"/scripts/*.sh

if [[ ! -f "$APP_DIR/.env" ]]; then
  cp "$APP_DIR/.env.example" "$APP_DIR/.env"
fi

mkdir -p "$APP_DIR/artifacts" "$APP_DIR/state/instances" "$APP_DIR/models/instances"
cd "$APP_DIR"
"$APP_DIR/.venv/bin/python" -m src.orchestrator --config "$APP_DIR/config/global.yaml" init-db

sed \
  -e "s|__APP_DIR__|$APP_DIR|g" \
  -e "s|__APP_USER__|$APP_USER|g" \
  "$APP_DIR/deploy/systemd/trader-swarm.service" | sudo tee /etc/systemd/system/trader-swarm.service >/dev/null
sed \
  -e "s|__APP_DIR__|$APP_DIR|g" \
  "$APP_DIR/deploy/systemd/trader-swarm.timer" | sudo tee /etc/systemd/system/trader-swarm.timer >/dev/null

sudo systemctl daemon-reload
sudo systemctl enable --now trader-swarm.timer

echo "Bootstrap complete."
echo "Manual run: $APP_DIR/scripts/run_cycle.sh"
echo "Health check: $APP_DIR/scripts/check_health.sh"
echo "Timer status: sudo systemctl status trader-swarm.timer"
