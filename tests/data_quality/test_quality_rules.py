"""Data-quality rules in isolation: the Bronze schema contract, the Pandera
schema gate's critical-vs-tolerable split, and the reconciliation severity
threshold."""

from __future__ import annotations

import pandas as pd
import pytest

from include.saas.quality import (
    assert_reconciliation_within_tolerance,
    validate_bronze_schema,
    validate_silver_schema,
)


def test_validate_bronze_schema_passes_with_all_required_columns():
    payload = {
        "customers": [
            {
                "customer_id": "cus_1",
                "company_name": "Acme",
                "segment": "SMB",
                "country": "US",
                "signup_date": "2026-01-01",
            }
        ],
        "subscriptions": [],
        "subscription_events": [],
        "invoices": [],
        "usage_events": [],
    }
    validate_bronze_schema(payload)  # must not raise


def test_validate_bronze_schema_raises_on_missing_required_column():
    payload = {
        "customers": [{"customer_id": "cus_1"}],  # missing company_name, segment, ...
        "subscriptions": [],
        "subscription_events": [],
        "invoices": [],
        "usage_events": [],
    }
    with pytest.raises(ValueError, match="Bronze schema contract violated"):
        validate_bronze_schema(payload)


def test_validate_bronze_schema_allows_empty_entities():
    """A zero-row entity has nothing to check — that's a quiet day, not an error."""
    payload = {
        "customers": [],
        "subscriptions": [],
        "subscription_events": [],
        "invoices": [],
        "usage_events": [],
    }
    validate_bronze_schema(payload)  # must not raise


def test_validate_silver_schema_quarantines_a_bad_row_but_is_not_critical():
    df = pd.DataFrame(
        [
            {
                "customer_id": "cus_1",
                "company_name": "Acme",
                "segment": "SMB",
                "country": "US",
                "signup_date": pd.Timestamp("2026-01-01"),
            },
            {
                "customer_id": "cus_2",
                "company_name": "Bad Segment Co",
                "segment": "NOT_A_REAL_SEGMENT",
                "country": "US",
                "signup_date": pd.Timestamp("2026-01-01"),
            },
        ]
    )
    report = validate_silver_schema("customers", df)

    assert report.is_critical_failure is False
    assert len(report.valid_df) == 1
    assert report.valid_df.iloc[0]["customer_id"] == "cus_1"
    assert report.has_failures is True


def test_validate_silver_schema_flags_null_critical_id_as_critical():
    df = pd.DataFrame(
        [
            {
                "customer_id": None,
                "company_name": "Acme",
                "segment": "SMB",
                "country": "US",
                "signup_date": pd.Timestamp("2026-01-01"),
            }
        ]
    )
    report = validate_silver_schema("customers", df)

    assert report.is_critical_failure is True


def test_assert_reconciliation_within_tolerance_passes_for_a_small_trickle():
    reconciliation_issues = pd.DataFrame([{"invoice_id": "inv_1"}])
    orphan_events = pd.DataFrame(columns=["event_id"])
    # 1 bad invoice out of 100 is well within the tolerated trickle.
    assert_reconciliation_within_tolerance(
        reconciliation_issues, orphan_events, total_paid_invoices=100, total_usage_events=1000
    )


def test_assert_reconciliation_raises_when_failure_ratio_is_implausibly_high():
    reconciliation_issues = pd.DataFrame([{"invoice_id": f"inv_{i}"} for i in range(50)])
    orphan_events = pd.DataFrame(columns=["event_id"])
    with pytest.raises(ValueError, match="reconcile to no active/past_due subscription"):
        assert_reconciliation_within_tolerance(
            reconciliation_issues, orphan_events, total_paid_invoices=100, total_usage_events=1000
        )
