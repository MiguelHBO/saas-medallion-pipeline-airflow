"""Shared fixtures: a small, fully hand-computed set of subscription_events
covering the main lifecycle scenarios (new customer, upgrade, downgrade,
churn, reactivation) so the MRR-waterfall math can be asserted against exact
expected numbers rather than "looks plausible"."""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_subscription_events() -> list[dict]:
    return [
        # New customer: trial converts directly to a paid Starter plan.
        {
            "event_id": "evt_0000001",
            "subscription_id": "sub_0000001",
            "event_type": "created",
            "event_date": "2026-01-02",
            "mrr_before": 0.0,
            "mrr_after": 100.0,
        },
        # Existing customer upgrades Growth -> Scale.
        {
            "event_id": "evt_0000002",
            "subscription_id": "sub_0000002",
            "event_type": "upgraded",
            "event_date": "2026-01-02",
            "mrr_before": 200.0,
            "mrr_after": 300.0,
        },
        # Existing customer downgrades Scale -> Growth.
        {
            "event_id": "evt_0000003",
            "subscription_id": "sub_0000003",
            "event_type": "downgraded",
            "event_date": "2026-01-02",
            "mrr_before": 500.0,
            "mrr_after": 350.0,
        },
        # Paying customer cancels outright (real churn, not a trial abandon).
        {
            "event_id": "evt_0000004",
            "subscription_id": "sub_0000004",
            "event_type": "canceled",
            "event_date": "2026-01-02",
            "mrr_before": 150.0,
            "mrr_after": 0.0,
        },
        # Previously canceled customer comes back.
        {
            "event_id": "evt_0000005",
            "subscription_id": "sub_0000005",
            "event_type": "reactivated",
            "event_date": "2026-01-02",
            "mrr_before": 0.0,
            "mrr_after": 80.0,
        },
    ]


@pytest.fixture
def sample_subscription_events_df(sample_subscription_events) -> pd.DataFrame:
    return pd.DataFrame(sample_subscription_events)


@pytest.fixture
def sample_subscriptions_yesterday() -> pd.DataFrame:
    """Start-of-period snapshot: 3 paying subscriptions, MRR baseline 850."""
    return pd.DataFrame(
        [
            {
                "subscription_id": "sub_0000002",
                "customer_id": "cus_0000002",
                "plan": "Growth",
                "mrr_amount": 200.0,
                "status": "active",
            },
            {
                "subscription_id": "sub_0000003",
                "customer_id": "cus_0000003",
                "plan": "Scale",
                "mrr_amount": 500.0,
                "status": "active",
            },
            {
                "subscription_id": "sub_0000004",
                "customer_id": "cus_0000004",
                "plan": "Starter",
                "mrr_amount": 150.0,
                "status": "active",
            },
        ]
    )


@pytest.fixture
def sample_subscriptions_today() -> pd.DataFrame:
    """End-of-period snapshot after the events above have been applied."""
    return pd.DataFrame(
        [
            {
                "subscription_id": "sub_0000001",
                "customer_id": "cus_0000001",
                "plan": "Starter",
                "mrr_amount": 100.0,
                "status": "active",
            },
            {
                "subscription_id": "sub_0000002",
                "customer_id": "cus_0000002",
                "plan": "Scale",
                "mrr_amount": 300.0,
                "status": "active",
            },
            {
                "subscription_id": "sub_0000003",
                "customer_id": "cus_0000003",
                "plan": "Growth",
                "mrr_amount": 350.0,
                "status": "active",
            },
            {
                "subscription_id": "sub_0000004",
                "customer_id": "cus_0000004",
                "plan": "Starter",
                "mrr_amount": 0.0,
                "status": "canceled",
            },
            {
                "subscription_id": "sub_0000005",
                "customer_id": "cus_0000005",
                "plan": "Starter",
                "mrr_amount": 80.0,
                "status": "active",
            },
        ]
    )
