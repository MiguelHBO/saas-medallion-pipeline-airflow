"""Adapter for the second common real-world origin: pulling directly from a
source system's database via a proper connection, instead of waiting for
someone to hand over a file.

Uses an Airflow Connection (``source_db``, provided via
``AIRFLOW_CONN_SOURCE_DB`` in docker-compose.yml) rather than any hardcoded
host/credentials — swapping this for a real production database later is a
connection-config change, not a code change.

Airflow is only imported inside :meth:`extract`, not at module load time, so
this module stays importable (e.g. by unit tests) in environments where
Airflow itself isn't installed.
"""

from __future__ import annotations

from datetime import date

from include.saas.constants import DATE_FMT
from include.saas.ingestion.base import BronzePayload, IngestionAdapter

# entity -> (column to drop after filtering, WHERE clause)
# customers/subscriptions are snapshot tables tagged with `snapshot_date` by
# the generator (see data_generator.write_snapshot_to_source_db) so that,
# like a real source with as-of support, a historical execution_date still
# resolves to that day's state — a plain "current state only" OLTP mirror
# would make backfilling these two entities meaningless.
_ENTITY_FILTERS: dict[str, tuple[str | None, str]] = {
    "customers": ("snapshot_date", "snapshot_date = %(day)s"),
    "subscriptions": ("snapshot_date", "snapshot_date = %(day)s"),
    "subscription_events": (None, "event_date = %(day)s"),
    "invoices": (None, "issued_date = %(day)s"),
    "usage_events": (None, "event_timestamp::date = %(day)s"),
}


class DbExtractAdapter(IngestionAdapter):
    def __init__(self, conn_id: str = "source_db") -> None:
        self.conn_id = conn_id

    def extract(self, execution_date: date) -> BronzePayload:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import inspect

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        engine = hook.get_sqlalchemy_engine()
        existing_tables = set(inspect(engine).get_table_names())

        if not existing_tables:
            raise RuntimeError(
                "source_db has no tables yet — run the data generator first "
                "(`python -m include.saas.data_generator`) to populate it."
            )

        day_str = execution_date.strftime(DATE_FMT)
        payload: BronzePayload = {}
        for entity, (drop_col, where_clause) in _ENTITY_FILTERS.items():
            if entity not in existing_tables:
                payload[entity] = []
                continue
            df = hook.get_pandas_df(
                f"SELECT * FROM {entity} WHERE {where_clause}",
                parameters={"day": day_str},
            )
            if drop_col and drop_col in df.columns:
                df = df.drop(columns=[drop_col])
            payload[entity] = df.to_dict(orient="records")
        return payload
