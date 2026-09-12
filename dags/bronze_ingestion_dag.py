"""Bronze layer: source-agnostic ingestion + schema-contract validation.

Runs on a real calendar schedule (``@daily`` by default, overridable via the
``schedule_override`` Airflow Variable) with ``catchup=True`` — the
historical backfill matters here: SaaS metrics only make sense as a time
series, and the data generator seeds N days of history precisely so this
DAG has something to backfill over.

No business logic lives in this file — every task is a thin call into
``include.saas``; the DAG only wires ordering, retries, and notifications.
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from include.notifications.slack import notify_failure, notify_pipeline_status
from include.saas.airflow_contracts import BRONZE_DATASET, LAST_BRONZE_DT_VARIABLE
from include.saas.ingestion import ingest, persist_bronze, read_bronze
from include.saas.quality import validate_bronze_schema

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

# Read once at parse time so a schedule change via the Variable doesn't need
# a code deploy. A DAG's `schedule` must be known at parse time, so this is
# one of the few places a Variable.get() at top level is the right call
# despite the usual "don't do expensive things in top-level DAG code" advice.
_SCHEDULE = Variable.get("schedule_override", default_var="") or "@daily"


@dag(
    dag_id="bronze_ingestion",
    description="Source-agnostic ingestion of the day's raw SaaS billing/product data.",
    schedule=_SCHEDULE,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["bronze", "saas-metrics"],
)
def bronze_ingestion_dag():
    @task
    def ingest_bronze(ds: str | None = None) -> str:
        """Execução: pull the day's payload from whichever origin is
        configured (``ingestion_source`` Variable: ``file_drop`` or
        ``db_extract``) and persist it as-is — no business-logic
        transformation, only the audit-trail write (spec §6.2)."""
        execution_date = date.fromisoformat(ds)
        source = Variable.get("ingestion_source", default_var="file_drop")
        payload = ingest(source, execution_date)
        persist_bronze(execution_date, payload)
        return ds

    @task(outlets=[BRONZE_DATASET])
    def validate_bronze(processed_ds: str) -> str:
        """Validação: re-read what was just persisted and check the minimum
        schema contract, independent of which adapter produced it. Only on
        success does this update the Bronze Dataset — downstream (Silver)
        should never be triggered by data that failed even this basic gate.

        Note the parameter name: it can't be ``ds`` — that's an Airflow-
        reserved context key, so a plain ``ds`` parameter can't also receive
        ``ingest_bronze``'s XCom return value positionally.
        """
        execution_date = date.fromisoformat(processed_ds)
        payload = read_bronze(execution_date)
        validate_bronze_schema(payload)
        Variable.set(LAST_BRONZE_DT_VARIABLE, processed_ds)
        return processed_ds

    @task(trigger_rule="all_done")
    def notify_status(**context) -> None:
        notify_pipeline_status(**context)

    validate_bronze(ingest_bronze()) >> notify_status()


bronze_ingestion_dag()
