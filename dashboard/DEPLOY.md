# Phase 7.5 Dashboard — Deploy Runbook

The Phase 7.5 dashboard adds Postgres + a React SPA to the existing
strategy_bot container. This runbook covers a clean cutover from the
Phase 7 image (just FastAPI healthcheck + cron) to 7.5.

## 1. Generate secrets

On the droplet (or any machine with `openssl`):

```bash
echo "JWT_SECRET=$(openssl rand -hex 32)"
echo "POSTGRES_PASSWORD=$(openssl rand -base64 24)"
echo "DASHBOARD_SEED_PASSWORD=$(openssl rand -base64 18)"
```

Save the seed password somewhere safe — it's the only login for the dashboard.

## 2. Update `.env` on the droplet

Append the new variables:

```
POSTGRES_PASSWORD=<from step 1>
DASHBOARD_SEED_USERNAME=sean
DASHBOARD_SEED_EMAIL=seanpcoleman1@gmail.com
DASHBOARD_SEED_PASSWORD=<from step 1>
JWT_SECRET=<from step 1>
DASHBOARD_COOKIE_SECURE=0
DATABASE_URL=postgresql://strategy_bot:<POSTGRES_PASSWORD>@postgres:5432/strategy_bot
```

`DASHBOARD_COOKIE_SECURE=0` is correct until you put HTTPS in front. Flip
to `1` after deploying Caddy/nginx + Let's Encrypt.

## 3. Schedule the cutover

The dashboard rebuild takes ~3-5 minutes (npm install + vite build + pip
install + alembic). Pick a time **outside** the cron windows:
- 23:00-23:30 ET: `scan_all` running
- 02:30-02:35 ET: `meta_ranker` + `watchlist_digest`
- 03:00 ET: `log_rotation`

Mid-day or early afternoon is safest.

## 4. Pull + rebuild

```bash
cd /home/bot/strategy_bot
git pull origin main
docker compose down
docker compose build       # ~3-5 min
docker compose up -d
```

## 5. Verify

```bash
# Both services should report healthy
docker compose ps

# Postgres should be reachable
docker compose exec postgres pg_isready -U strategy_bot

# Alembic should report at the head revision
docker compose exec strategy_bot alembic -c dashboard/alembic/alembic.ini current

# Health endpoint
curl -fsS http://localhost:8000/api/health

# Login (replace with your seed creds)
curl -fsS -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"sean","password":"<seed password>"}' \
  -c /tmp/cookies

curl -fsS http://localhost:8000/api/auth/me -b /tmp/cookies
# -> {"id":1,"username":"sean",...}

# Dashboard root should serve the SPA
curl -sS http://localhost:8000/ | head -5
# -> <!doctype html>...

# Cron jobs still loaded
docker compose exec strategy_bot crontab -l
```

Open `http://134.209.64.159:8000/` in a browser, log in, verify Today,
Watchlist, History, Notifications, Settings all render.

## 6. Rollback if anything fails

The Phase 7 image is still tagged in your local Docker history. Bring
back the previous version:

```bash
cd /home/bot/strategy_bot
git log --oneline -10                  # find the last Phase 7 SHA
git checkout <sha>
docker compose down
docker compose build
docker compose up -d
```

The Postgres volume (`postgres_data`) persists across rollbacks — your
notifications + audit log survive. If you need to nuke the DB entirely,
`docker compose down -v` removes the volume.

## 7. After-deploy verification

- The Notifications page should show every alert from existing
  `logs/strategy_bot_*.jsonl` files (the JSONL backfill runs on first
  startup; subsequent dashboard restarts are no-ops).
- The Settings page should list all 14 scanners with weights matching
  `config/scanner_weights.yaml`. Edit one (e.g., bump
  `congressional_trades` from 1.0 to 1.05), click Save, then verify:
  - The YAML file on disk shows the new weight.
  - The audit log table at the bottom of the Settings page shows the change.
  - The next `meta_ranker` run uses the new weight.
- The Today page should match `scan_output/<latest>/master_ranked.csv`.
  Click any ticker — Ticker Detail should load (initially without a
  reverse index; once meta_ranker runs again, history populates).

## Open follow-ups (not blocking)

- Add HTTPS via Caddy + Let's Encrypt; set `DASHBOARD_COOKIE_SECURE=1`.
- Lock down port 8000 behind UFW once HTTPS proxy is in place.
- The "Refresh from yfinance" button on Ticker Detail bypasses the
  scheduled cron cache; consider rate-limiting if it sees heavy use.
