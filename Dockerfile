FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip \
    && pip install \
        dbt-core \
        dbt-duckdb \
        dbt-bigquery \
        dbt-redshift \
        dbt-snowflake \
        dbt-sqlserver \
        uv

RUN uv pip install --system ingestr

COPY ingestion.py .

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/usr/local/bin:${PATH}"

# Entrypoint
ENTRYPOINT ["python3","ingestion.py"]
