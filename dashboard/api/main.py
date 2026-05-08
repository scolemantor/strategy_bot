"""FastAPI app for the strategy_bot dashboard.

Mounts the existing /api/health router from src/api/health.py and the new
dashboard routers under /api/*. Static SPA assets served from
/app/dashboard/web/dist/ (built by the Node stage in Dockerfile).

Startup hooks:
  1. require_jwt_secret() — crash loudly if JWT_SECRET unset
  2. seed_user_if_empty()  — first-run user seed from env vars
  3. PushoverDispatcher notifications hook registration (Phase 7.5 commit 5)

This module is run via `uvicorn dashboard.api.main:app` in entrypoint.sh.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from dashboard.api.auth import require_jwt_secret
from dashboard.api.db import get_sessionmaker
from dashboard.api.jsonl_backfill import backfill_if_empty
from dashboard.api.routes import auth as auth_routes
from dashboard.api.routes import history as history_routes
from dashboard.api.routes import notifications as notifications_routes
from dashboard.api.routes import settings as settings_routes
from dashboard.api.routes import ticker as ticker_routes
from dashboard.api.routes import today as today_routes
from dashboard.api.routes import watchlist as watchlist_routes
from dashboard.api.seed import seed_user_if_empty

log = logging.getLogger(__name__)

# Existing healthcheck app — its routes get re-registered on this app.
from src.api.health import health as health_route, ready as ready_route  # noqa: E402

SPA_DIST_DIR = Path("/app/dashboard/web/dist")


@asynccontextmanager
async def lifespan(app: FastAPI):
    require_jwt_secret()
    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        seed_user_if_empty(db)
        backfill_if_empty(db)
    finally:
        db.close()
    yield


app = FastAPI(title="strategy_bot dashboard", version="0.7.5", lifespan=lifespan)

# Health endpoints (no auth required) — re-export the same handlers used
# by Phase 7's standalone src/api/health.py app.
app.add_api_route("/api/health", health_route, methods=["GET"])
app.add_api_route("/api/health/ready", ready_route, methods=["GET"])

# Auth router (login is unauthenticated; logout/me handle auth themselves
# via the current_user dependency).
app.include_router(auth_routes.router)
app.include_router(today_routes.router)
app.include_router(watchlist_routes.router)
app.include_router(ticker_routes.router)
app.include_router(history_routes.router)
app.include_router(notifications_routes.router)
app.include_router(settings_routes.router)


# Static SPA — only mounted in production where /app/dashboard/web/dist
# exists (created by the multi-stage Dockerfile build). For local dev
# without Vite-built assets, this is a no-op.
if SPA_DIST_DIR.exists():
    assets_dir = SPA_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/{full_path:path}")
    async def spa_catchall(full_path: str):
        # API routes are matched first; this only fires for non-API paths.
        if full_path.startswith("api/"):
            return FileResponse(SPA_DIST_DIR / "index.html", status_code=404)
        candidate = SPA_DIST_DIR / full_path
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(SPA_DIST_DIR / "index.html")


def main() -> None:
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
