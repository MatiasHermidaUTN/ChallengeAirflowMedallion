"""DAG diario: CSV de ventas -> Bronze (Python) -> Silver/Gold (dbt) con tests de calidad."""
from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_DIR = Path(os.environ.get("RAW_DATA_DIR", "/opt/airflow/data/raw"))
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_CMD = f"cd {DBT_DIR} && dbt"  # profiles.yml vive en el mismo directorio
DBT_FLAGS = "--profiles-dir ."

BRONZE_COLUMNS = [
    "order_id", "order_date", "customer_id", "product",
    "quantity", "unit_price", "currency", "status",
]


def ingest_bronze(ds: str, **_) -> int:
    """Carga el CSV del dia tal cual viene (todo texto) en bronze.raw_sales.

    Idempotente: borra lo ya cargado para ese archivo antes de insertar.
    """
    path = RAW_DIR / f"sales_{ds}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo esperado: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        rows = [[r.get(c) for c in BRONZE_COLUMNS] for r in csv.DictReader(f)]

    hook = PostgresHook(postgres_conn_id="warehouse")
    hook.run("CREATE SCHEMA IF NOT EXISTS bronze")
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS bronze.raw_sales (
            order_id text, order_date text, customer_id text, product text,
            quantity text, unit_price text, currency text, status text,
            _source_file text, _ingested_at timestamptz DEFAULT now()
        )
        """
    )
    hook.run("DELETE FROM bronze.raw_sales WHERE _source_file = %s", parameters=(path.name,))
    hook.insert_rows(
        table="bronze.raw_sales",
        rows=[r + [path.name] for r in rows],
        target_fields=BRONZE_COLUMNS + ["_source_file"],
    )
    return len(rows)


default_args = {
    "owner": "data_eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_medallion",
    description="Pipeline Medallion de ventas diarias (Bronze/Silver/Gold)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["medallion", "dbt", "sales"],
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
