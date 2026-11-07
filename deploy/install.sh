#!/usr/bin/env bash
# Install the collector on a fresh Debian/Ubuntu VM (e.g. Hetzner CX22). Run as root from the repo.
set -euo pipefail

APP=/opt/dbdelay

id dbdelay >/dev/null 2>&1 || useradd --system --create-home --home-dir /home/dbdelay --shell /usr/sbin/nologin dbdelay
apt-get update && apt-get install -y --no-install-recommends unattended-upgrades curl git rsync ca-certificates
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

mkdir -p "$APP"
rsync -a --delete --exclude .venv --exclude .python --exclude data --exclude .env ./ "$APP"/
mkdir -p "$APP/data"
if [ ! -f "$APP/.env" ]; then
  cp "$APP/.env.example" "$APP/.env"
  sed -i 's#^DBDELAY_DATA_DIR=.*#DBDELAY_DATA_DIR=/opt/dbdelay/data#' "$APP/.env"
  chown dbdelay:dbdelay "$APP/.env"; chmod 600 "$APP/.env"
  echo "Created $APP/.env: fill in the DB keys (and Telegram), then re-run this script."
  exit 1
fi
chown -R dbdelay:dbdelay "$APP"
chmod 600 "$APP/.env"

cd "$APP" && runuser -u dbdelay -- env UV_PYTHON_INSTALL_DIR="$APP/.python" uv sync --no-dev --frozen

cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now dbdelay-collector.service dbdelay-health.timer dbdelay-daily-report.timer
systemctl --no-pager status dbdelay-collector.service
