# Remplacer python:3.11-slim par une version avec tag fixe
FROM python:3.11.9-slim-bookworm

WORKDIR /app

# Bookworm (Debian 12) est stable, pas Trixie (Debian 13 unstable)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "app.py"]