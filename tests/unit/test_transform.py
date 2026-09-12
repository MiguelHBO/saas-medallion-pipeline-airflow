"""Silver transform tests: type normalization, dedup, and the two
reconciliation checks (paid invoice vs. active subscription, orphan usage
event vs. known customer)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from include.saas.transform import transform


def _bronze_payload(**overrides):
    payload = {
        "customers": [
            {
                "customer_id": "cus_1",
                "company_name": "Acme Inc",
                "segment": "SMB",
                "country": "US",
                "signup_date": "2026-01-01",
            }
        ],
        "subscriptions": [
            {
                "subscription_id": "sub_1",
                "customer_id": "cus_1",
                "plan": "Starter",
                "mrr_amount": 99.0,
                "status": "active",
                "start_date": "2026-01-01",
                "end_date": None,
                "billing_cycle": "monthly",
            }
        ],
        "subscription_events": [],
        "invoices": [],
        "usage_events": [],
    }
    payload.update(overrides)
    return payload


def test_transform_normalizes_dates_and_money():
    payload = _bronze_payload()
    result = transform(date(2026, 1, 1), payload)

    customers = result.tables["customers"]
    assert pd.api.types.is_datetime64_any_dtype(customers["signup_date"])

    subscriptions = result.tables["subscriptions"]
    assert subscriptions.loc[0, "mrr_amount"] == 99.0


def test_transform_dedupes_by_primary_key_keeping_the_last_record():
    payload = _bronze_payload(
        customers=[
            {
                "customer_id": "cus_1",
                "company_name": "Acme Inc (typo)",
                "segment": "SMB",
                "country": "US",
                "signup_date": "2026-01-01",
            },
            {
                "customer_id": "cus_1",
                "company_name": "Acme Inc",
                "segment": "SMB",
                "country": "US",
                "signup_date": "2026-01-01",
            },
        ]
    )
    result = transform(date(2026, 1, 1), payload)
    customers = result.tables["customers"]

    assert len(customers) == 1
    assert customers.iloc[0]["company_name"] == "Acme Inc"


def test_transform_quarantines_tolerable_failures_without_raising():
    payload = _bronze_payload(
        subscriptions=[
            {
                "subscription_id": "sub_1",
                "customer_id": "cus_1",
                "plan": "Starter",
                "mrr_amount": 99.0,
                "status": "active",
                "start_date": "2026-01-01",
                "end_date": None,
                "billing_cycle": "monthly",
            },
            {
                "subscription_id": "sub_2",
                "customer_id": "cus_1",
                "plan": "Starter",
                "mrr_amount": -50.0,  # dirty: negative MRR
                "status": "active",
                "start_date": "2026-01-01",
                "end_date": None,
                "billing_cycle": "monthly",
            },
        ]
    )
    result = transform(date(2026, 1, 1), payload)

    # The bad row is dropped from the usable table, not fatal to the run.
    assert len(result.tables["subscriptions"]) == 1
    assert result.tables["subscriptions"].iloc[0]["subscription_id"] == "sub_1"


def test_transform_flags_paid_invoice_with_no_active_subscription():
    payload = _bronze_payload(
        subscriptions=[
            {
                "subscription_id": "sub_1",
                "customer_id": "cus_1",
                "plan": "Starter",
                "mrr_amount": 0.0,
                "status": "canceled",
                "start_date": "2026-01-01",
                "end_date": "2026-01-05",
                "billing_cycle": "monthly",
            }
        ],
        invoices=[
            {
                "invoice_id": "inv_1",
                "customer_id": "cus_1",
                "subscription_id": "sub_1",
                "amount": 99.0,
                "status": "paid",
                "issued_date": "2026-01-10",
                "paid_date": "2026-01-10",
            }
        ],
    )
    result = transform(date(2026, 1, 10), payload)

    assert len(result.reconciliation_issues) == 1
    assert result.reconciliation_issues.iloc[0]["invoice_id"] == "inv_1"


def test_transform_flags_orphan_usage_event():
    payload = _bronze_payload(
        usage_events=[
            {
                "event_id": "use_1",
                "customer_id": "cus_unknown",
                "user_id": "cus_unknown_user_001",
                "feature": "dashboard",
                "event_timestamp": "2026-01-01T10:00:00",
            }
        ]
    )
    result = transform(date(2026, 1, 1), payload)

    assert len(result.orphan_usage_events) == 1
    assert result.orphan_usage_events.iloc[0]["customer_id"] == "cus_unknown"
