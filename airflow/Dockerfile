FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update --allow-releaseinfo-change && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir --timeout 1000 -r requirements.txt