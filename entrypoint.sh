#!/bin/bash
# Phase 7 / 7.5 container entrypoint.
# Runs as root (so it can install crontab, start cron daemon, run alembic).
# Order:
#   1. Wait for Postgres to be ready (compose healthcheck handles most of this).
#   2. Run alembic migrations to current head.
#   3. Generate + install crontab, start cron daemon (Phase 7).
#   4. Exec uvicorn dashboard.api.main:app as the bot user.
set -euo pipefail

LOG_DIR=${LOG_DIR:-/app/logs}
APP_DIR=/app

# Ensure log dirs exist and are owned by bot. Bind-mounted volumes may have
# host ownership; chown succeeds because we're root inside the container.
mkdir -p "${LOG_DIR}/cron" "${APP_DIR}/scan_output" "${APP_DIR}/backtest_output"
chown -R bot:bot "${LOG_DIR}" "${APP_DIR}/scan_output" "${APP_DIR}/backtest_output" || true

# Phase 7.5: wait for Postgres + run migrations.
if [ -n "${DATABASE_URL:-}" ]; then
  echo "[entrypoint] waiting for postgres (DATABASE_URL set)"
  for i in $(seq 1 30); do
    if python -c "import psycopg2,os,urllib.parse as u; \
p=u.urlparse(os.environ['DATABASE_URL']); \
psycopg2.connect(host=p.hostname,port=p.port or 5432,user=p.username,password=p.password,dbname=p.path.lstrip('/')).close()" 2>/dev/null; then
      echo "[entrypoint] postgres reachable"
      break
    fi
    sleep 2
  done

  echo "[entrypoint] running alembic upgrade head"
  cd "${APP_DIR}"
  alembic -c dashboard/alembic/alembic.ini upgrade head
else
  echo "[entrypoint] DATABASE_URL not set; skipping postgres wait + alembic"
fi

# Generate the cron schedule from yaml. cron_generator outputs commands
# wrapped in `su bot -c '...'` so individual jobs run as bot.
python -m src.deploy.cron_generator --output /tmp/crontab_jobs.txt

# Prepend env vars cron jobs need (PUSHOVER_*, ALPACA_*, SEC_*, LOG_DIR, DATABASE_URL).
# Cron's standard parser exposes KEY=value lines as env to all jobs;
# `su` (without dash) inherits them down to the scanner processes.
{
  printenv | grep -E '^(PUSHOVER_|ALPACA_|SEC_|LOG_DIR=|DATABASE_URL=)' || true
  cat /tmp/crontab_jobs.txt
} > /tmp/crontab.txt

# Install for root (cron daemon runs as root; jobs su to bot).
crontab /tmp/crontab.txt
echo "[entrypoint] Installed crontab:"
crontab -l | sed -E 's/^([A-Z_]+=).*/\1***REDACTED***/' | sed 's/^/  /'

# Start Debian cron daemon in the background.
cron
echo "[entrypoint] cron daemon started"

# Replace the entrypoint process with uvicorn (running as bot).
echo "[entrypoint] starting uvicorn (dashboard.api.main:app) as bot user"
exec su bot -c "cd ${APP_DIR} && python -m uvicorn dashboard.api.main:app --host 0.0.0.0 --port 8000"
