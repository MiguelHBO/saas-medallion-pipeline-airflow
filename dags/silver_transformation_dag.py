"""Silver layer: clean, dedupe, and reconcile the day Bronze most recently validated.

Scheduled on the Bronze ``Dataset`` (data-aware scheduling per spec §6.1)
rather than its own cron — this is Datasets doing exactly what they're
designed for: "run Silver right after Bronze produces something new,"
without hardcoding a time-of-day guess for when Bronze happens to finish.

One deliberate limitation, worth being upfront about rather than glossing
over: Dataset-triggered runs coalesce multiple upstream updates that land
between scheduler evaluations, so during the *initial* historical backfill
(bronze_ingestion catching up on N days almost back-to-back) this DAG will
not automatically get one run per historical day — only the steady-state,
one-day-at-a-time case is. After bronze_ingestion's initial backfill
finishes, backfill this DAG explicitly for the same date range:
    airflow dags backfill silver_transformation -s <start> -e <end>
This is a real, documented characteristic of Dataset scheduling, not a bug
to work around — Datasets are event-driven triggers, not a historical-replay
mechanism.

To know exactly *which* day to process (a dataset-triggered run's own
logical date is when it was triggered, not the date the upstream DAG run was
for), this DAG reads the ``last_bronze_dt`` Variable that bronze_ingestion's
validation task sets, rather than trusting its own ``{{ ds }}``.
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from include.notifications.slack import notify_failure, notify_pipeline_status
from include.saas.airflow_contracts import (
    BRONZE_DATASET,
    LAST_BRONZE_DT_VARIABLE,
    LAST_SILVER_DT_VARIABLE,
    SILVER_DATASET,
)
from include.saas.ingestion import read_bronze
from include.saas.quality import (
    assert_reconciliation_within_tolerance,
    find_orphan_usage_events,
    reconcile_invoices_vs_subscriptions,
)
from include.saas.transform import persist, read_silver_table, transform

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="silver_transformation",
    description="Clean, dedupe, and reconcile Bronze into typed Silver Parquet tables.",
    schedule=[BRONZE_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # moot for Dataset scheduling; see backfill note above
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "saas-metrics"],
)
def silver_transformation_dag():
    @task
    def transform_silver() -> str:
        """Execução: normalize types, dedupe, run the Pandera schema gate
        (tolerable failures quarantined, critical failures raise here), and
        persist every entity to Parquet."""
        ds = Variable.get(LAST_BRONZE_DT_VARIABLE)
        execution_date = date.fromisoformat(ds)
        bronze_payload = read_bronze(execution_date)
        result = transform(execution_date, bronze_payload)
        persist(result)
        return ds

    @task(outlets=[SILVER_DATASET])
    def validate_silver(processed_ds: str) -> str:
        """Validação: re-read the persisted Silver tables and re-run the
        cross-entity reconciliation checks (spec §6.3) against them —
        independent of whatever `transform_silver` already saw, so this
        task is checking what's actually on disk, not trusting its sibling.

        Parameter can't be named ``ds`` — that's an Airflow-reserved context
        key and would conflict with receiving ``transform_silver``'s XCom
        return value positionally.
        """
        execution_date = date.fromisoformat(processed_ds)
        invoices = read_silver_table("invoices", execution_date)
        subscriptions = read_silver_table("subscriptions", execution_date)
        usage_events = read_silver_table("usage_events", execution_date)
        customers = read_silver_table("customers", execution_date)

        reconciliation_issues = reconcile_invoices_vs_subscriptions(invoices, subscriptions)
        orphan_events = find_orphan_usage_events(usage_events, customers)
        assert_reconciliation_within_tolerance(
            reconciliation_issues,
            orphan_events,
            total_paid_invoices=(
                int((invoices["status"] == "paid").sum()) if not invoices.empty else 0
            ),
            total_usage_events=len(usage_events),
        )

        Variable.set(LAST_SILVER_DT_VARIABLE, processed_ds)
        return processed_ds

    @task(trigger_rule="all_done")
    def notify_status(**context) -> None:
        notify_pipeline_status(**context)

    validate_silver(transform_silver()) >> notify_status()


silver_transformation_dag()
