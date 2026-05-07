#!/bin/bash
# Phase 7 container entrypoint.
# Runs as root (so it can install crontab and start cron daemon).
# Generates the crontab, prepends env vars cron jobs need, installs it,
# starts cron in the background, then exec's into uvicorn as the bot user
# (uvicorn becomes the foreground process so tini can forward signals).
set -euo pipefail

LOG_DIR=${LOG_DIR:-/app/logs}
APP_DIR=/app

# Ensure log dirs exist and are owned by bot. Bind-mounted volumes may have
# host ownership; chown succeeds because we're root inside the container.
mkdir -p "${LOG_DIR}/cron" "${APP_DIR}/scan_output" "${APP_DIR}/backtest_output"
chown -R bot:bot "${LOG_DIR}" "${APP_DIR}/scan_output" "${APP_DIR}/backtest_output" || true

# Generate the cron schedule from yaml. cron_generator outputs commands
# wrapped in `su bot -c '...'` so individual jobs run as bot.
python -m src.deploy.cron_generator --output /tmp/crontab_jobs.txt

# Prepend env vars cron jobs need (PUSHOVER_*, ALPACA_*, SEC_*, LOG_DIR).
# Cron's standard parser exposes KEY=value lines as env to all jobs;
# `su` (without dash) inherits them down to the scanner processes.
{
  printenv | grep -E '^(PUSHOVER_|ALPACA_|SEC_|LOG_DIR=)' || true
  cat /tmp/crontab_jobs.txt
} > /tmp/crontab.txt

# Install for root (cron daemon runs as root; jobs su to bot).
crontab /tmp/crontab.txt
echo "[entrypoint] Installed crontab:"
crontab -l | sed 's/^/  /'

# Start Debian cron daemon in the background.
cron
echo "[entrypoint] cron daemon started (pid $(pgrep -x cron | head -1))"

# Replace the entrypoint process with uvicorn (running as bot).
# `exec` is critical here: it makes uvicorn the child of tini (PID 1),
# so SIGTERM from `docker stop` propagates correctly.
echo "[entrypoint] starting uvicorn as bot user"
exec su bot -c "cd ${APP_DIR} && python -m src.api.health"
