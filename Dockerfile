FROM apache/airflow:2.8.1-python3.11

# ── System deps ──────────────────────────────────────────────
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ── Python deps – pinned to versions compatible with Airflow 2.8.1 ──
# Airflow 2.8.1 constraints:
#   sqlalchemy < 2.0  (uses 1.4.x)
#   colorlog   < 5.0
USER airflow
RUN pip install --no-cache-dir \
        "pandas==2.1.4" \
        "sqlalchemy==1.4.51" \
        "psycopg2-binary==2.9.9" \
        "python-dotenv==1.0.0" \
        "lxml==5.1.0" \
        "xmltodict==0.13.0" \
        "requests==2.31.0" \
        "faker==22.2.0" \
        "colorlog==4.8.0" \
        "tabulate==0.9.0" \
        "numpy==1.26.3" \
        "matplotlib==3.8.2"

ENV PYTHONPATH=/opt/airflow
