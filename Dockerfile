# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Non-root user with /data write access (rarely changes, so cached early)
RUN useradd --system --no-create-home meshirc && \
    mkdir -p /data && chown meshirc /data

# Install deps first for layer caching; BuildKit cache mount reuses downloads
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# Copy application code (all modules, so new ones aren't forgotten)
COPY *.py ./

USER meshirc

ENTRYPOINT ["python", "meshirc.py"]
