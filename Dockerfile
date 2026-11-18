# syntax=docker/dockerfile:1
#
# Two stages: the builder installs the hash-locked dependencies and the
# package into a virtualenv; the runtime image copies only that virtualenv,
# so no build tools, pip or setuptools ship in it.
#
# Base pinned by digest: the multi-arch index of python:3.11-slim as of
# 2026-11-05. Bump it deliberately (and re-run the Trivy gate in CI).
ARG PYTHON_IMAGE=python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

FROM ${PYTHON_IMAGE} AS builder
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ONLY_BINARY=:all:
WORKDIR /build

# Dependencies first: this layer is rebuilt only when the lock changes.
# --only-binary: every locked dependency has a wheel, so no compiler is needed.
RUN python -m venv /opt/venv
COPY requirements.txt requirements-build.txt ./
RUN /opt/venv/bin/pip install --require-hashes -r requirements.txt \
    && python -m venv /opt/build \
    && /opt/build/bin/pip install --require-hashes -r requirements-build.txt

# Then the package, built with the hash-locked build backend.
COPY pyproject.toml README.md ./
COPY api ./api
COPY config ./config
COPY core ./core
COPY evaluation ./evaluation
COPY migrations ./migrations
COPY models ./models
COPY scripts ./scripts
RUN /opt/build/bin/pip wheel --no-deps --no-build-isolation --wheel-dir /build/dist . \
    && /opt/venv/bin/pip install --no-deps /build/dist/*.whl \
    && /opt/venv/bin/pip uninstall -y pip setuptools

FROM ${PYTHON_IMAGE} AS runtime
# Security fixes the pinned base predates, each pinned to the exact Debian
# version that fixes it (Trivy gates the image in CI; drop a line once the
# base digest is bumped past it):
#   zlib1g  CVE-2026-85091  fixed in 1:1.3.dfsg+really1.3.1-1+deb13u1
# The base image's own pip/setuptools are not needed at runtime and carry
# their own advisories; nothing is installed after this point.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        "zlib1g=1:1.3.dfsg+really1.3.1-1+deb13u1" \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip uninstall -y pip setuptools wheel \
    && useradd --create-home --shell /usr/sbin/nologin --uid 1000 appuser \
    && mkdir -p /app/models /app/logs \
    && chown -R appuser:appuser /app
COPY --from=builder /opt/venv /opt/venv
# Logs are one JSON object per line for the container's log collector
# (override with LOG_FORMAT=text); see docs/observability.md.
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_FORMAT=json
WORKDIR /app
USER appuser

EXPOSE 8000
# Liveness of the API process: /health/live checks no dependency, so a
# database or RPC outage does not mark a working container unhealthy (an
# orchestrator would restart it for nothing). Readiness is /health/ready.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen(f\"http://127.0.0.1:{os.environ.get('API_PORT', '8000')}/health/live\", timeout=4)"]

# The API server (binds 127.0.0.1 unless API_HOST and authentication are set;
# see docs/api.md). One-shot commands: docker run <image> wash-audit ...
CMD ["wash-api"]
