#!/bin/bash
set -e

echo "[update] Stopping bot..."
systemctl stop weatherbot

echo "[update] Backing up secrets..."
cp /root/weatherapp/.env /tmp/weatherapp-env-backup 2>/dev/null || true

echo "[update] Removing old app..."
rm -rf /root/weatherapp

echo "[update] Unzipping new version..."
unzip -o /root/weatherapp.zip -d /root/weatherapp

cd /root/weatherapp

echo "[update] Restoring secrets..."
cp /tmp/weatherapp-env-backup .env 2>/dev/null || true

echo "[update] Creating venv..."
python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt

echo "[update] Starting bot..."
systemctl start weatherbot

echo "[update] Done!"
systemctl status weatherbot --no-pager
