"""Silver layer: type normalization, dedup, cross-entity reconciliation, and
persistence to Parquet.

Business logic lives here, not in the DAG — ``dags/silver_transformation_dag.py``
only orchestrates calls into :func:`transform` and :func:`persist`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from include.saas.constants import DATE_FMT, ENTITY_NAMES
from include.saas.ingestion.base import BronzePayload
from include.saas.quality import (
    ValidationReport,
    find_orphan_usage_events,
    reconcile_invoices_vs_subscriptions,
    validate_silver_schema,
    write_quality_report,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = REPO_ROOT / "include" / "data" / "silver"

# Pure-date columns normalized to `datetime64[ns]` (midnight). event_timestamp
# is handled separately since it carries a real time-of-day component.
DATE_COLUMNS: dict[str, list[str]] = {
    "customers": ["signup_date"],
    "subscriptions": ["start_date", "end_date"],
    "subscription_events": ["event_date"],
    "invoices": ["issued_date", "paid_date"],
    "usage_events": [],
}

MONEY_COLUMNS = ("mrr_amount", "mrr_before", "mrr_after", "amount")

DEDUP_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "subscriptions": "subscription_id",
    "subscription_events": "event_id",
    "invoices": "invoice_id",
    "usage_events": "event_id",
}


def _normalize_types(entity: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    for col in DATE_COLUMNS.get(entity, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if entity == "usage_events" and "event_timestamp" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    for col in MONEY_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    return df


def _dedupe(entity: str, df: pd.DataFrame) -> pd.DataFrame:
    key = DEDUP_KEYS[entity]
    if df.empty or key not in df.columns:
        return df
    before = len(df)
    deduped = df.drop_duplicates(subset=[key], keep="last")
    removed = before - len(deduped)
    if removed:
        logger.warning("%s: dropped %d duplicate row(s) keyed on %s.", entity, removed, key)
    return deduped


@dataclass
class SilverResult:
    execution_date: date
    tables: dict[str, pd.DataFrame]
    reports: list[ValidationReport]
    reconciliation_issues: pd.DataFrame
    orphan_usage_events: pd.DataFrame


def transform(execution_date: date, bronze_payload: BronzePayload) -> SilverResult:
    """Clean + validate one day's Bronze payload into Silver-ready tables.

    Raises ``ValueError`` if any entity has a *critical* data-quality
    failure (see ``include.saas.quality.CRITICAL_COLUMNS``) — everything
    else is quarantined (dropped from the returned table, recorded in the
    quality report) rather than failing the run.
    """
    reports: list[ValidationReport] = []
    tables: dict[str, pd.DataFrame] = {}

    for entity in ENTITY_NAMES:
        df = pd.DataFrame(bronze_payload.get(entity, []))
        df = _normalize_types(entity, df)
        df = _dedupe(entity, df)
        report = validate_silver_schema(entity, df)
        reports.append(report)
        if report.is_critical_failure:
            raise ValueError(
                f"{entity}: critical data-quality failure on {execution_date} — aborting Silver."
            )
        tables[entity] = report.valid_df

    reconciliation_issues = reconcile_invoices_vs_subscriptions(
        tables["invoices"], tables["subscriptions"]
    )
    orphan_usage_events = find_orphan_usage_events(tables["usage_events"], tables["customers"])

    if not reconciliation_issues.empty:
        logger.warning(
            "%d paid invoice(s) reconcile to no active/past_due subscription on %s.",
            len(reconciliation_issues),
            execution_date,
        )
    if not orphan_usage_events.empty:
        logger.warning(
            "%d usage event(s) reference an unknown customer_id on %s.",
            len(orphan_usage_events),
            execution_date,
        )

    write_quality_report(execution_date, reports)

    return SilverResult(
        execution_date=execution_date,
        tables=tables,
        reports=reports,
        reconciliation_issues=reconciliation_issues,
        orphan_usage_events=orphan_usage_events,
    )


def persist(result: SilverResult, base_dir: Path = SILVER_DIR) -> dict[str, Path]:
    """Write every Silver table to ``base_dir/<entity>/dt=<execution_date>/part-0.parquet``."""
    written: dict[str, Path] = {}
    dt_str = result.execution_date.strftime(DATE_FMT)
    for entity, df in result.tables.items():
        entity_dir = base_dir / entity / f"dt={dt_str}"
        entity_dir.mkdir(parents=True, exist_ok=True)
        out_path = entity_dir / "part-0.parquet"
        df.to_parquet(out_path, index=False)
        written[entity] = out_path
    return written


def read_silver_table(
    entity: str, execution_date: date, base_dir: Path = SILVER_DIR
) -> pd.DataFrame:
    """Read one entity's Parquet partition for a given day (used by Gold)."""
    path = base_dir / entity / f"dt={execution_date.strftime(DATE_FMT)}" / "part-0.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No Silver partition for {entity} on {execution_date} at {path}")
    return pd.read_parquet(path)
