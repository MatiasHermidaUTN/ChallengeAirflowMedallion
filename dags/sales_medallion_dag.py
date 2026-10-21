"""DAG diario: transactions_raw.csv -> Bronze (Python) -> Silver/Gold (dbt) con tests de calidad."""
from __future__ import annotations

import csv
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_FILE = Path(os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw")) / "transactions_raw.csv"
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_CMD = f"cd {DBT_DIR} && dbt"  # profiles.yml vive en el mismo directorio
DBT_FLAGS = "--profiles-dir ."

# Todo llega como string; el tipado se hace en Silver.
BRONZE_COLUMNS = ["transaction_id", "customer_id", "transaction_date", "amount", "status", "country_code"]


def ingest_bronze(ds: str, **_) -> int:
    """Carga transactions_raw.csv tal cual en bronze.raw_transactions.

    Bronze es append-only y esta particionada por fecha de ingesta (_ingestion_date): cada dia
    escribe su propia particion. Reintentar el mismo dia reemplaza solo la particion de ese dia.
    """
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo esperado: {RAW_FILE}")

    ingestion_date = date.fromisoformat(ds)
    partition = f"bronze.raw_transactions_{ingestion_date:%Y%m%d}"  # derivado de una fecha validada

    with RAW_FILE.open(newline="", encoding="utf-8") as f:
        rows = [[r.get(c) for c in BRONZE_COLUMNS] + [RAW_FILE.name, ds] for r in csv.DictReader(f)]

    hook = PostgresHook(postgres_conn_id="warehouse")
    hook.run("CREATE SCHEMA IF NOT EXISTS bronze")
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
            transaction_id text, customer_id text, transaction_date text,
            amount text, status text, country_code text,
            _source_file text,
            _ingestion_date date NOT NULL,
            _ingested_at timestamptz NOT NULL DEFAULT now()
        ) PARTITION BY RANGE (_ingestion_date)
        """
    )
    hook.run(
        f"CREATE TABLE IF NOT EXISTS {partition} PARTITION OF bronze.raw_transactions "
        "FOR VALUES FROM (%s) TO (%s)",
        parameters=(ingestion_date, ingestion_date + timedelta(days=1)),
    )
    hook.run(f"TRUNCATE {partition}")
    hook.insert_rows(
        table="bronze.raw_transactions",
        rows=rows,
        target_fields=BRONZE_COLUMNS + ["_source_file", "_ingestion_date"],
    )
    return len(rows)


default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_medallion",
    description="Pipeline Medallion de transacciones diarias (Bronze/Silver/Gold)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["medallion", "dbt", "transactions"],
) as dag:
    bronze = PythonOperator(task_id="ingest_bronze", python_callable=ingest_bronze)

    silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"{DBT_CMD} run --select silver {DBT_FLAGS}",
    )

    silver_tests = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"{DBT_CMD} test --select silver {DBT_FLAGS}",
    )

    gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"{DBT_CMD} run --select gold {DBT_FLAGS}",
    )

    gold_tests = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"{DBT_CMD} test --select gold {DBT_FLAGS}",
    )

    # Gold solo corre si Silver paso las validaciones de calidad.
    bronze >> silver >> silver_tests >> gold >> gold_tests
