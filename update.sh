#!/bin/bash
set -e

APP_DIR="/opt/weatherapp"
ENV_BACKUP="/tmp/weatherapp-env-backup"

echo "[update] Stopping bot..."
systemctl stop weatherbot || true

echo "[update] Updating from git..."
cd "$APP_DIR"

# Backup local secrets before any destructive operations
cp "$APP_DIR/.env" "$ENV_BACKUP" 2>/dev/null || true

# Remove generated artefacts
rm -rf bot/__pycache__

# Pull latest changes (or reset to origin/main if local drift exists)
git fetch origin
git reset --hard "origin/$(git rev-parse --abbrev-ref HEAD)"

# Restore secrets
cp "$ENV_BACKUP" "$APP_DIR/.env" 2>/dev/null || true

echo "[update] Installing dependencies..."
source .venv/bin/activate
pip install -q -r requirements.txt

echo "[update] Starting bot..."
systemctl start weatherbot

echo "[update] Done!"
systemctl status weatherbot --no-pager
