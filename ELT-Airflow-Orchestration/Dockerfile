FROM apache/airflow:2.8.1

# Install additional Python packages needed for ETL
# Note: Using pandas 2.0.3 (compatible with Python 3.8 in base image)
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    requests==2.31.0 \
    apache-airflow-providers-postgres

USER airflow