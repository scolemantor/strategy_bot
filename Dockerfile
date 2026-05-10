# Phase 7.5 production image. Multi-stage:
#   1. Node 20 Alpine builds the React SPA -> /build/dist
#   2. Python 3.11 slim runtime; receives the built SPA into
#      /app/dashboard/web/dist for FastAPI to serve.

# --- Stage 1: frontend build ---
FROM node:20-alpine AS frontend-build
WORKDIR /build
COPY dashboard/web/package.json dashboard/web/package-lock.json ./
# `npm install` (not `npm ci`) — the lockfile was generated on Windows and
# omits @rollup/rollup-linux-x64-musl. `npm ci` would refuse to install
# platform binaries missing from the lockfile; `npm install` resolves
# optionals for the current platform. To restore strict reproducibility,
# regenerate package-lock.json inside a node:20-alpine container so the
# linux-musl deps are pinned, then switch back to `npm ci`.
# Ref: https://github.com/npm/cli/issues/4828
RUN npm install --no-audit --no-fund
COPY dashboard/web/ ./
RUN npm run build

# --- Stage 2: python runtime ---
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps: build-essential for compiling wheels (psycopg2-binary should
# avoid this but other deps may need it), cron for the scheduler, curl for
# the HEALTHCHECK probe, ca-certificates for HTTPS, tini for PID 1 signal
# forwarding, tzdata for cron's /etc/localtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cron \
    curl \
    ca-certificates \
    tini \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/America/New_York /etc/localtime && \
    echo America/New_York > /etc/timezone

RUN groupadd --gid 1000 bot && \
    useradd --uid 1000 --gid bot --create-home --shell /bin/bash bot

WORKDIR /app

# Install Python deps first (separate cache layer from app code)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=bot:bot . /app

# Receive the React build from stage 1
COPY --from=frontend-build --chown=bot:bot /build/dist /app/dashboard/web/dist

RUN mkdir -p /app/logs/cron && chown -R bot:bot /app/logs

COPY --chown=root:root entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
