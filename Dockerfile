FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copia o seu arquivo de dependências
COPY requirements.txt /requirements.txt

# Instala polars, py7zr, sqlalchemy, etc.
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow