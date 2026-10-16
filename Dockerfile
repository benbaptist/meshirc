FROM python:3.12-slim

WORKDIR /app

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY meshirc.py commands.py ./

# Non-root user with /data write access
RUN useradd --system --no-create-home meshirc && \
    mkdir -p /data && chown meshirc /data
USER meshirc

ENTRYPOINT ["python", "meshirc.py"]
