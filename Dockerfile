# ═══════════════════════════════════════════════════════════════
# AEGIS Trading System - Backend Dockerfile
# Multi-stage build for Python backend services
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# Stage 1: Base Python image with system dependencies
# ═══════════════════════════════════════════════════════════════
FROM python:3.13-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# ═══════════════════════════════════════════════════════════════
# Stage 2: Dependencies installation
# ═══════════════════════════════════════════════════════════════
FROM base as dependencies

WORKDIR /app

# Copy requirements files
COPY backend/requirements.txt ./backend/
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r backend/requirements.txt && \
    pip install -r requirements.txt && \
    pip install pathway>=0.7.0 python-dotenv redis pymongo alpaca-py

# ═══════════════════════════════════════════════════════════════
# Stage 3: Application
# ═══════════════════════════════════════════════════════════════
FROM dependencies as application

WORKDIR /app

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p agent_logs backend/logs reports data

# Set permissions
RUN chmod +x parallel_full_pipeline_clean.py historical_data.py

# ═══════════════════════════════════════════════════════════════
# Stage 4: Production (Final)
# ═══════════════════════════════════════════════════════════════
FROM application as production

# Expose ports
EXPOSE 8000 6379

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import redis; r = redis.Redis(host='localhost', port=6379); r.ping()" || exit 1

# Default command
CMD ["python", "parallel_full_pipeline_clean.py", "--single", "--quick"]