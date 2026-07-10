FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# pyproject.toml is the single source of truth for dependencies (see requirements.txt
# header), so the image installs the package directly from it rather than a separate,
# driftable requirements.txt copy step.
COPY . .
RUN pip install --no-cache-dir .

# Create directories if they don't exist, and run as a non-root user
RUN mkdir -p /app/models /app/logs \
    && useradd --create-home --shell /bin/bash --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "scripts/run_audit.py", "--help"]
