"""Gold layer: MRR/ARR/churn/NRR (``saas_metrics_daily``) and product
engagement (``product_engagement_daily``) for the day Silver most recently
validated.

Scheduled on the Silver ``Dataset``, same rationale and same historical-
backfill caveat as ``silver_transformation_dag.py`` — see that file's
docstring. After bronze/silver have finished their initial backfill:
    airflow dags backfill gold_metrics -s <start> -e <end>
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from include.notifications.slack import notify_failure, notify_pipeline_status
from include.saas.airflow_contracts import (
    GOLD_DATASET,
    LAST_GOLD_DT_VARIABLE,
    LAST_SILVER_DT_VARIABLE,
    SILVER_DATASET,
)
from include.saas.metrics import (
    assert_mrr_reconciliation,
    compute_product_engagement_daily,
    compute_saas_metrics_daily,
    load_gold_inputs,
    persist_gold,
    read_gold_metrics_row,
)
from include.saas.transform import read_silver_table

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="gold_metrics",
    description="Daily MRR/ARR/churn/NRR and product engagement metrics.",
    schedule=[SILVER_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,  # moot for Dataset scheduling; see silver_transformation_dag.py
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "saas-metrics"],
)
def gold_metrics_dag():
    @task(
        # The task the finance/growth team actually waits on every morning —
        # a concrete, defensible reason to attach an SLA to this one task
        # rather than the DAG as a whole.
        sla=timedelta(hours=3),
    )
    def compute_gold_metrics() -> str:
        """Execução: read Silver, compute both Gold tables, persist."""
        ds = Variable.get(LAST_SILVER_DT_VARIABLE)
        execution_date = date.fromisoformat(ds)
        inputs, usage_events_window = load_gold_inputs(execution_date)
        saas_metrics_row = compute_saas_metrics_daily(inputs)
        engagement_df = compute_product_engagement_daily(
            execution_date, usage_events_window, inputs.subscriptions_today
        )
        persist_gold(execution_date, saas_metrics_row, engagement_df)
        return ds

    @task(outlets=[GOLD_DATASET])
    def validate_gold_reconciliation(processed_ds: str) -> str:
        """Validação: the end-to-end check spec §6.4 asks for — re-read what
        was just persisted and confirm Gold's mrr_total matches the sum of
        Silver's active/past_due subscriptions for the same day.

        Parameter can't be named ``ds`` — that's an Airflow-reserved context
        key and would conflict with receiving ``compute_gold_metrics``'s
        XCom return value positionally.
        """
        execution_date = date.fromisoformat(processed_ds)
        subscriptions_today = read_silver_table("subscriptions", execution_date)
        saas_metrics_row = read_gold_metrics_row(execution_date)
        assert_mrr_reconciliation(subscriptions_today, saas_metrics_row)
        Variable.set(LAST_GOLD_DT_VARIABLE, processed_ds)
        return processed_ds

    @task(trigger_rule="all_done")
    def notify_status(**context) -> None:
        notify_pipeline_status(**context)

    validate_gold_reconciliation(compute_gold_metrics()) >> notify_status()


gold_metrics_dag()
