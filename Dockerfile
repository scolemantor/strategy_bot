# Phase 7 production image. Linux, Python 3.11.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps: build-essential for any wheel that needs compilation,
# cron for the in-container scheduler, curl for the HEALTHCHECK probe,
# ca-certificates for HTTPS, tini for proper PID 1 signal forwarding.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cron \
    curl \
    ca-certificates \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for runtime.
RUN groupadd --gid 1000 bot && \
    useradd --uid 1000 --gid bot --create-home --shell /bin/bash bot

WORKDIR /app

# Install Python deps first (cache layer separate from app code).
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the repo (filtered by .dockerignore).
COPY --chown=bot:bot . /app

# Ensure the cron log dir exists in the image baseline (the entrypoint also
# creates it for bind-mount cases, but having it here means non-bind-mounted
# volumes work too).
RUN mkdir -p /app/logs/cron && chown -R bot:bot /app/logs

# Entrypoint script handles startup: generate crontab, install it, start
# cron daemon in background, exec uvicorn as bot. Marked +x at copy time.
COPY --chown=root:root entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Note: do NOT set USER bot here. Entrypoint runs as root (cron requires it)
# and drops to bot for uvicorn via su. Individual cron jobs also su to bot.

EXPOSE 8000

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
