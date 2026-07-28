FROM python:3.12-slim

WORKDIR /app

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY meshirc.py .

# Non-root user
RUN useradd --system --no-create-home meshirc
USER meshirc

ENTRYPOINT ["python", "meshirc.py"]
