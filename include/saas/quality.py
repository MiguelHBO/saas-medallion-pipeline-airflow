"""Data-quality gate for the pipeline.

Two layers, matching the two places the spec asks for a check:

1. :func:`validate_bronze_schema` — a cheap "does this even have the columns
   we need" contract check, run right after ingestion, before Silver is
   allowed to touch the data. Any origin (file_drop or db_extract) must pass
   the same contract.
2. Pandera schemas (below) + :func:`validate_silver_schema` — proper
   per-column typing/domain/non-null checks run in Silver. Failures are
   split into **critical** (a required identifier is null/duplicated, or the
   schema is structurally broken -> fail the task) and **tolerable** (a stray
   null/negative MRR value, an orphan usage event -> log + write a report,
   let the pipeline continue). This mirrors how a real data team actually
   triages data-quality issues: not everything wrong with the data is worth
   stopping the pipeline for.

Chose **Pandera** over Great Expectations for this project: schemas are
plain Python objects colocated with the pandas/Parquet code that already
does the rest of Silver, no separate suite/checkpoint/data-docs
infrastructure to maintain solo. Great Expectations' richer profiling and
auto-generated documentation are the trade-off given up.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

from include.saas.constants import (
    BILLING_CYCLES,
    DATE_FMT,
    INVOICE_STATUSES,
    PLANS,
    SEGMENTS,
    SUBSCRIPTION_EVENT_TYPES,
    SUBSCRIPTION_STATUSES,
)
from include.saas.ingestion.base import BronzePayload

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
QUALITY_REPORTS_DIR = REPO_ROOT / "include" / "data" / "quality_reports"

# --- Bronze contract ---------------------------------------------------

BRONZE_REQUIRED_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "company_name", "segment", "country", "signup_date"],
    "subscriptions": [
        "subscription_id",
        "customer_id",
        "plan",
        "mrr_amount",
        "status",
        "start_date",
        "billing_cycle",
    ],
    "subscription_events": [
        "event_id",
        "subscription_id",
        "event_type",
        "event_date",
        "mrr_before",
        "mrr_after",
    ],
    "invoices": [
        "invoice_id",
        "customer_id",
        "subscription_id",
        "amount",
        "status",
        "issued_date",
    ],
    "usage_events": ["event_id", "customer_id", "user_id", "feature", "event_timestamp"],
}


def validate_bronze_schema(payload: BronzePayload) -> None:
    """Raise ``ValueError`` if any non-empty entity is missing a required
    column. An entity with zero records for the day is not itself an error —
    there may simply have been nothing new."""
    errors: list[str] = []
    for entity, required_cols in BRONZE_REQUIRED_COLUMNS.items():
        records = payload.get(entity, [])
        if not records:
            continue
        present = set(records[0].keys())
        missing = [c for c in required_cols if c not in present]
        if missing:
            errors.append(f"{entity}: missing required column(s) {missing}")
    if errors:
        raise ValueError("Bronze schema contract violated: " + "; ".join(errors))


# --- Silver Pandera schemas ---------------------------------------------

# Date/datetime columns are validated as `datetime64[ns]` (Silver normalizes
# them from Bronze's date/ISO strings before validation runs — see
# transform._normalize_types) so Gold can do real date arithmetic without
# re-parsing strings.

CUSTOMERS_SCHEMA = DataFrameSchema(
    {
        "customer_id": Column(str, unique=True, nullable=False),
        "company_name": Column(str, nullable=False),
        "segment": Column(str, Check.isin(SEGMENTS), nullable=False),
        "country": Column(str, nullable=False),
        "signup_date": Column("datetime64[ns]", nullable=False),
    },
    coerce=True,
)

SUBSCRIPTIONS_SCHEMA = DataFrameSchema(
    {
        "subscription_id": Column(str, unique=True, nullable=False),
        "customer_id": Column(str, nullable=False),
        "plan": Column(str, Check.isin(PLANS), nullable=False),
        "mrr_amount": Column(float, Check.ge(0), nullable=False),
        "status": Column(str, Check.isin(SUBSCRIPTION_STATUSES), nullable=False),
        "start_date": Column("datetime64[ns]", nullable=False),
        "end_date": Column("datetime64[ns]", nullable=True),
        "billing_cycle": Column(str, Check.isin(BILLING_CYCLES), nullable=False),
    },
    coerce=True,
)

SUBSCRIPTION_EVENTS_SCHEMA = DataFrameSchema(
    {
        "event_id": Column(str, unique=True, nullable=False),
        "subscription_id": Column(str, nullable=False),
        "event_type": Column(str, Check.isin(SUBSCRIPTION_EVENT_TYPES), nullable=False),
        "event_date": Column("datetime64[ns]", nullable=False),
        "mrr_before": Column(float, Check.ge(0), nullable=False),
        "mrr_after": Column(float, Check.ge(0), nullable=False),
    },
    coerce=True,
)

INVOICES_SCHEMA = DataFrameSchema(
    {
        "invoice_id": Column(str, unique=True, nullable=False),
        "customer_id": Column(str, nullable=False),
        "subscription_id": Column(str, nullable=False),
        "amount": Column(float, Check.ge(0), nullable=False),
        "status": Column(str, Check.isin(INVOICE_STATUSES), nullable=False),
        "issued_date": Column("datetime64[ns]", nullable=False),
        "paid_date": Column("datetime64[ns]", nullable=True),
    },
    coerce=True,
)

USAGE_EVENTS_SCHEMA = DataFrameSchema(
    {
        "event_id": Column(str, unique=True, nullable=False),
        "customer_id": Column(str, nullable=False),
        "user_id": Column(str, nullable=False),
        "feature": Column(str, nullable=False),
        "event_timestamp": Column("datetime64[ns]", nullable=False),
    },
    coerce=True,
)

SILVER_SCHEMAS: dict[str, DataFrameSchema] = {
    "customers": CUSTOMERS_SCHEMA,
    "subscriptions": SUBSCRIPTIONS_SCHEMA,
    "subscription_events": SUBSCRIPTION_EVENTS_SCHEMA,
    "invoices": INVOICES_SCHEMA,
    "usage_events": USAGE_EVENTS_SCHEMA,
}

# Columns whose failure means the row/entity cannot be trusted at all —
# anything else (a dirty MRR value, an orphan usage event) is logged and
# quarantined instead of failing the task.
CRITICAL_COLUMNS: dict[str, set[str]] = {
    "customers": {"customer_id"},
    "subscriptions": {"subscription_id", "customer_id"},
    "subscription_events": {"event_id", "subscription_id"},
    "invoices": {"invoice_id", "customer_id", "subscription_id"},
    "usage_events": {"event_id"},
}


@dataclass
class ValidationReport:
    entity: str
    is_critical_failure: bool
    valid_df: pd.DataFrame
    failure_cases: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def has_failures(self) -> bool:
        return not self.failure_cases.empty


def validate_silver_schema(entity: str, df: pd.DataFrame) -> ValidationReport:
    """Validate ``df`` against the Pandera schema for ``entity``.

    Rows that fail a non-critical check are dropped from ``valid_df`` and
    recorded in ``failure_cases`` rather than failing the run. Any failure on
    a :data:`CRITICAL_COLUMNS` column marks the report as critical — callers
    should raise in that case.
    """
    schema = SILVER_SCHEMAS[entity]
    critical_cols = CRITICAL_COLUMNS[entity]

    if df.empty:
        return ValidationReport(entity=entity, is_critical_failure=False, valid_df=df)

    try:
        valid_df = schema.validate(df, lazy=True)
        return ValidationReport(entity=entity, is_critical_failure=False, valid_df=valid_df)
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases
        failed_columns = set(failure_cases["column"].dropna().unique())
        is_critical = bool(failed_columns & critical_cols)

        bad_indices = set(failure_cases["index"].dropna().astype(int))
        valid_df = df.drop(index=[i for i in bad_indices if i in df.index], errors="ignore")

        logger.warning(
            "%s: %d row(s) failed validation (%s). Critical=%s.",
            entity,
            len(bad_indices),
            sorted(failed_columns),
            is_critical,
        )
        return ValidationReport(
            entity=entity,
            is_critical_failure=is_critical,
            valid_df=valid_df,
            failure_cases=failure_cases,
        )


def write_quality_report(execution_date: date, reports: list[ValidationReport]) -> Path | None:
    """Persist every non-empty failure report for the day as a single JSON
    file under ``include/data/quality_reports/``."""
    entries = {
        r.entity: json.loads(r.failure_cases.to_json(orient="records"))
        for r in reports
        if r.has_failures
    }
    if not entries:
        return None

    QUALITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = QUALITY_REPORTS_DIR / f"{execution_date.strftime(DATE_FMT)}.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, indent=2, default=str)
    logger.info("Wrote data-quality report to %s", out_path)
    return out_path


def reconcile_invoices_vs_subscriptions(
    invoices_df: pd.DataFrame, subscriptions_df: pd.DataFrame
) -> pd.DataFrame:
    """Every ``paid`` invoice should belong to a subscription that is
    currently ``active`` or ``past_due``. Returns the offending invoice rows
    (empty if none) — this is the reconciliation check spec §6.3 asks for."""
    if invoices_df.empty or subscriptions_df.empty:
        return invoices_df.iloc[0:0]

    paid = invoices_df[invoices_df["status"] == "paid"]
    valid_sub_ids = set(
        subscriptions_df.loc[
            subscriptions_df["status"].isin(["active", "past_due"]), "subscription_id"
        ]
    )
    return paid[~paid["subscription_id"].isin(valid_sub_ids)]


def find_orphan_usage_events(
    usage_events_df: pd.DataFrame, customers_df: pd.DataFrame
) -> pd.DataFrame:
    """Usage events referencing a ``customer_id`` that doesn't exist in
    ``customers`` — the orphan-event edge case from the data generator."""
    if usage_events_df.empty:
        return usage_events_df
    known_ids = set(customers_df.get("customer_id", pd.Series(dtype=str)))
    return usage_events_df[~usage_events_df["customer_id"].isin(known_ids)]


# A handful of phantom invoices or orphan events is the deliberate, expected
# trickle of bad data the generator injects (~1% of rows) — worth logging and
# quarantining, not worth failing Silver over. A much higher share would mean
# something is actually broken upstream (a joined table wiped, a botched
# origin swap), which *is* worth stopping the pipeline for.
RECONCILIATION_FAILURE_RATIO_THRESHOLD = 0.2


def assert_reconciliation_within_tolerance(
    reconciliation_issues: pd.DataFrame,
    orphan_usage_events: pd.DataFrame,
    total_paid_invoices: int,
    total_usage_events: int,
) -> None:
    """Raise if either reconciliation problem affects an implausibly large
    share of the day's rows — the Silver "validação" task's hard-failure gate."""
    if total_paid_invoices and len(reconciliation_issues) / total_paid_invoices > (
        RECONCILIATION_FAILURE_RATIO_THRESHOLD
    ):
        raise ValueError(
            f"{len(reconciliation_issues)}/{total_paid_invoices} paid invoices reconcile to no "
            "active/past_due subscription — that's far above the expected trickle of bad data."
        )
    if total_usage_events and len(orphan_usage_events) / total_usage_events > (
        RECONCILIATION_FAILURE_RATIO_THRESHOLD
    ):
        raise ValueError(
            f"{len(orphan_usage_events)}/{total_usage_events} usage events reference an unknown "
            "customer_id — that's far above the expected trickle of bad data."
        )
