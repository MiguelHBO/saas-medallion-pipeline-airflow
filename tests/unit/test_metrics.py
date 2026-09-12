"""Deterministic MRR-waterfall tests: given a fixed, hand-computed set of
subscription_events, New/Expansion/Contraction/Churned MRR (and everything
derived from them) must match an exact expected value — this is the part of
the pipeline most likely to get probed in an interview, so it gets the most
scrutiny in the test suite."""

from __future__ import annotations

from datetime import date

import pandas as pd

from include.saas.metrics import (
    GoldMetricsInputs,
    assert_mrr_reconciliation,
    compute_mrr_waterfall,
    compute_product_engagement_daily,
    compute_saas_metrics_daily,
)


def test_mrr_waterfall_matches_hand_computed_values(sample_subscription_events_df):
    waterfall = compute_mrr_waterfall(sample_subscription_events_df)

    assert waterfall == {
        "new_mrr": 180.0,  # created (100) + reactivated (80): both start from a $0 baseline
        "expansion_mrr": 100.0,  # upgraded: 300 - 200
        "contraction_mrr": 150.0,  # downgraded: 500 - 350
        "churned_mrr": 150.0,  # canceled: 150 - 0
    }


def test_mrr_waterfall_on_empty_events_is_all_zero():
    empty = pd.DataFrame(
        columns=[
            "event_id",
            "subscription_id",
            "event_type",
            "event_date",
            "mrr_before",
            "mrr_after",
        ]
    )
    assert compute_mrr_waterfall(empty) == {
        "new_mrr": 0.0,
        "expansion_mrr": 0.0,
        "contraction_mrr": 0.0,
        "churned_mrr": 0.0,
    }


def test_saas_metrics_daily_matches_hand_computed_values(
    sample_subscription_events_df, sample_subscriptions_today, sample_subscriptions_yesterday
):
    inputs = GoldMetricsInputs(
        execution_date=date(2026, 1, 2),
        subscriptions_today=sample_subscriptions_today,
        subscriptions_yesterday=sample_subscriptions_yesterday,
        subscription_events_today=sample_subscription_events_df,
    )
    row = compute_saas_metrics_daily(inputs)

    assert row["mrr_total"] == 830.0
    assert row["arr_total"] == 830.0 * 12
    assert row["new_mrr"] == 180.0
    assert row["expansion_mrr"] == 100.0
    assert row["contraction_mrr"] == 150.0
    assert row["churned_mrr"] == 150.0
    assert row["customers_active"] == 4
    assert row["customers_start_of_period"] == 3
    assert row["customer_churn_rate"] == round(1 / 3, 4)
    assert row["revenue_churn_rate"] == round(150 / 850, 4)
    assert row["nrr"] == round((850 + 100 - 150 - 150) / 850, 4)
    assert row["arpu"] == round(830.0 / 4, 2)


def test_saas_metrics_daily_without_prior_day_returns_none_for_period_over_period_fields(
    sample_subscription_events_df, sample_subscriptions_today
):
    """The very first day of history has no D-1 baseline — churn rate,
    revenue churn rate, and NRR must come back as None, not a misleading 0."""
    inputs = GoldMetricsInputs(
        execution_date=date(2026, 1, 1),
        subscriptions_today=sample_subscriptions_today,
        subscriptions_yesterday=None,
        subscription_events_today=sample_subscription_events_df,
    )
    row = compute_saas_metrics_daily(inputs)

    assert row["customer_churn_rate"] is None
    assert row["revenue_churn_rate"] is None
    assert row["nrr"] is None
    assert row["customers_start_of_period"] is None
    # mrr_total/arpu don't need a prior-day baseline and should still compute.
    assert row["mrr_total"] == 830.0
    assert row["arpu"] == round(830.0 / 4, 2)


def test_assert_mrr_reconciliation_passes_when_consistent(sample_subscriptions_today):
    assert_mrr_reconciliation(sample_subscriptions_today, {"dt": "2026-01-02", "mrr_total": 830.0})


def test_assert_mrr_reconciliation_raises_when_inconsistent(sample_subscriptions_today):
    try:
        assert_mrr_reconciliation(
            sample_subscriptions_today, {"dt": "2026-01-02", "mrr_total": 999.0}
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("expected assert_mrr_reconciliation to raise on a mismatch")


def test_product_engagement_daily_dau_wau_mau_and_at_risk_flag():
    execution_date = date(2026, 1, 15)
    usage_events = pd.DataFrame(
        [
            # cus_active: used the product today, this week, this month.
            {"customer_id": "cus_active", "event_timestamp": "2026-01-15T09:00:00"},
            # cus_dormant: last seen well outside even the MAU window.
            {"customer_id": "cus_dormant", "event_timestamp": "2025-11-01T09:00:00"},
            # cus_at_risk: heavy usage 8-14 days ago, almost nothing the last 7 days,
            # and (per subscriptions_today below) currently past_due.
            {"customer_id": "cus_at_risk", "event_timestamp": "2026-01-05T09:00:00"},
            {"customer_id": "cus_at_risk", "event_timestamp": "2026-01-06T09:00:00"},
            {"customer_id": "cus_at_risk", "event_timestamp": "2026-01-07T09:00:00"},
            {"customer_id": "cus_at_risk", "event_timestamp": "2026-01-08T09:00:00"},
        ]
    )
    subscriptions_today = pd.DataFrame(
        [
            {"customer_id": "cus_active", "status": "active"},
            {"customer_id": "cus_dormant", "status": "active"},
            {"customer_id": "cus_at_risk", "status": "past_due"},
        ]
    )

    result = compute_product_engagement_daily(
        execution_date, usage_events, subscriptions_today
    ).set_index("customer_id")

    assert result.loc["cus_active", ["dau", "wau", "mau"]].tolist() == [1, 1, 1]
    assert not result.loc["cus_active", "at_risk"]

    assert result.loc["cus_dormant", ["dau", "wau", "mau"]].tolist() == [0, 0, 0]
    assert not result.loc["cus_dormant", "at_risk"]

    assert result.loc["cus_at_risk", "dau"] == 0
    assert result.loc["cus_at_risk", "at_risk"]
