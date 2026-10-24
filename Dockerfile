# ==============================================================================
# HyperQuery-LSM: Embedded Log-Structured Merge-Tree Redis-Compatible Database
# ==============================================================================
FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY hyperquery ./hyperquery
RUN pip install --no-cache-dir -e .

RUN mkdir -p /data
VOLUME /data

# Standard Redis Port
EXPOSE 6379

ENTRYPOINT ["hyperquery", "server", "--host", "0.0.0.0", "--port", "6379", "--data", "/data"]
