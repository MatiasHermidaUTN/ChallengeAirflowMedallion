FROM apache/airflow:2.9.3-python3.11

USER airflow
RUN pip install --no-cache-dir dbt-postgres==1.8.2 apache-airflow-providers-postgres
