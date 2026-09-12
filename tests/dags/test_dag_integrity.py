"""DAG integrity checks: every DAG file must import without error, contain
no cycles, and have retries configured.

Needs a real Airflow install and an initialized metadata DB (our DAGs read
an Airflow Variable at parse time for the schedule override), so this suite
runs inside the Airflow container / CI's Airflow job, not the plain
pandas/pandera venv used for tests/unit and tests/data_quality:
    airflow db migrate   # once, if not already done
    pytest tests/dags
"""

from __future__ import annotations

import pytest
from airflow.models import DagBag

EXPECTED_DAG_IDS = {"bronze_ingestion", "silver_transformation", "gold_metrics"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)


def test_dagbag_has_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}


def test_all_expected_dags_are_present(dagbag: DagBag):
    assert set(dagbag.dags.keys()) >= EXPECTED_DAG_IDS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_no_cycles(dagbag: DagBag, dag_id: str):
    dag = dagbag.dags[dag_id]
    dag.topological_sort()  # raises AirflowDagCycleException on a cycle


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_retries_configured(dagbag: DagBag, dag_id: str):
    dag = dagbag.dags[dag_id]
    for task in dag.tasks:
        assert (
            task.retries and task.retries > 0
        ), f"{dag_id}.{task.task_id} has no retries configured"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_on_failure_callback(dagbag: DagBag, dag_id: str):
    dag = dagbag.dags[dag_id]
    for task in dag.tasks:
        assert (
            task.on_failure_callback is not None
        ), f"{dag_id}.{task.task_id} has no on_failure_callback"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_every_dag_ends_with_an_all_done_notify_task(dagbag: DagBag, dag_id: str):
    dag = dagbag.dags[dag_id]
    notify_tasks = [t for t in dag.tasks if t.task_id == "notify_status"]
    assert len(notify_tasks) == 1
    assert notify_tasks[0].trigger_rule == "all_done"
