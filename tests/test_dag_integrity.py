"""Chequeos basicos del DAG. Requieren apache-airflow instalado (si no, se saltean)."""
import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_task_order(dagbag):
    dag = dagbag.get_dag("sales_medallion")
    order = [t.task_id for t in dag.topological_sort()]
    assert order == [
        "ingest_bronze", "dbt_run_silver", "dbt_test_silver", "dbt_run_gold", "dbt_test_gold",
    ]


def test_gold_runs_only_after_silver_tests(dagbag):
    dag = dagbag.get_dag("sales_medallion")
    assert "dbt_test_silver" in dag.get_task("dbt_run_gold").upstream_task_ids
