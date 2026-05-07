# Phase 7 production image. Linux, Python 3.11.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps: build-essential for any wheel that needs compilation,
# curl for the HEALTHCHECK probe, ca-certificates for HTTPS.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
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

USER bot

EXPOSE 8000

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/health || exit 1

# Default to bash for manual debugging. docker-compose overrides to run uvicorn.
CMD ["bash"]
