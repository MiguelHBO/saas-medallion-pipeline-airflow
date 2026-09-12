"""Shared Airflow wiring constants: Dataset URIs and Variable names that more
than one DAG needs to agree on.

Deliberately kept out of the ``dags/`` files themselves: importing one DAG
file from another re-executes its whole module body (including the
``@dag``-decorated factory call), which can register the same DAG twice
under Airflow's DagFileProcessor — a well-known Airflow footgun. Constants
shared across DAGs belong in ``include/``, like everything else that isn't
pure orchestration.
"""

from __future__ import annotations

from airflow.datasets import Dataset

BRONZE_DATASET = Dataset("saas://bronze/daily")
SILVER_DATASET = Dataset("saas://silver/daily")
GOLD_DATASET = Dataset("saas://gold/daily")

LAST_BRONZE_DT_VARIABLE = "last_bronze_dt"
LAST_SILVER_DT_VARIABLE = "last_silver_dt"
LAST_GOLD_DT_VARIABLE = "last_gold_dt"
