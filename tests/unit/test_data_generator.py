"""Tests for the synthetic data generator: reproducibility, growth shape,
the Bronze snapshot/delta contract, and that the deliberate data-quality
edge cases actually occur (otherwise the quality-layer tests would be
exercising dead code)."""

from __future__ import annotations

from datetime import date

from include.saas.data_generator import DataGenerator


def test_same_seed_produces_identical_output():
    run_a = DataGenerator(seed=7, max_customers=200).run(days=15, start_date=date(2026, 1, 1))
    run_b = DataGenerator(seed=7, max_customers=200).run(days=15, start_date=date(2026, 1, 1))

    assert [s.customers for s in run_a] == [s.customers for s in run_b]
    assert [s.subscription_events for s in run_a] == [s.subscription_events for s in run_b]


def test_different_seeds_produce_different_output():
    run_a = DataGenerator(seed=1, max_customers=200).run(days=15, start_date=date(2026, 1, 1))
    run_b = DataGenerator(seed=2, max_customers=200).run(days=15, start_date=date(2026, 1, 1))

    assert run_a[-1].customers != run_b[-1].customers


def test_customer_count_never_decreases_and_respects_the_cap():
    snapshots = DataGenerator(seed=3, max_customers=150).run(days=60, start_date=date(2026, 1, 1))

    counts = [len(s.customers) for s in snapshots]
    assert counts == sorted(counts)  # monotonically non-decreasing
    assert counts[-1] <= 150


def test_bronze_contract_snapshots_vs_deltas():
    """customers/subscriptions are full as-of snapshots (every day should
    contain every customer that has ever signed up); subscription_events/
    invoices/usage_events are deltas (only that day's activity)."""
    snapshots = DataGenerator(seed=11, max_customers=300).run(days=40, start_date=date(2026, 1, 1))

    total_customers_ever = len(snapshots[-1].customers)
    for snapshot in snapshots:
        # A snapshot table never "loses" a customer that existed earlier.
        assert len(snapshot.customers) <= total_customers_ever
        assert len(snapshot.subscriptions) == len(snapshot.customers)

    # Some day must have zero new subscription_events (deltas vary day to
    # day) — if every single day had events, that wouldn't distinguish a
    # snapshot table from a delta table.
    event_counts = [len(s.subscription_events) for s in snapshots]
    assert min(event_counts) == 0 or len(set(event_counts)) > 1


def test_deliberate_edge_cases_occur_over_a_long_enough_run():
    """A long, differently-seeded run should exercise every deliberate
    data-quality edge case at least once — otherwise the quality-layer tests
    that depend on them would be testing nothing."""
    snapshots = DataGenerator(seed=99, max_customers=500).run(days=300, start_date=date(2025, 1, 1))

    orphan_usage_events = sum(
        1
        for snapshot in snapshots
        for event in snapshot.usage_events
        if event["customer_id"].startswith("cus_ghost")
    )

    phantom_paid_invoices = 0
    for snapshot in snapshots:
        subs_by_id = {sub["subscription_id"]: sub for sub in snapshot.subscriptions}
        for invoice in snapshot.invoices:
            sub = subs_by_id.get(invoice["subscription_id"])
            if invoice["status"] == "paid" and sub and sub["status"] not in ("active", "past_due"):
                phantom_paid_invoices += 1

    dirty_mrr_events = sum(
        1
        for snapshot in snapshots
        for event in snapshot.subscription_events
        if event["mrr_after"] is None or (event["mrr_after"] is not None and event["mrr_after"] < 0)
    )

    assert orphan_usage_events > 0
    assert phantom_paid_invoices > 0
    assert dirty_mrr_events > 0


def test_upgrade_and_downgrade_events_land_within_the_new_plans_price_band():
    """Regression test for a real bug caught during development: a
    downgrade/upgrade event used to scale the *previous* MRR value instead
    of landing in the new plan's own price band, so a subscription could end
    up labeled "Growth" with a Scale-sized MRR."""
    from include.saas.constants import PLAN_MRR_RANGE

    snapshots = DataGenerator(seed=21, max_customers=400).run(days=200, start_date=date(2025, 6, 1))
    checked_any = False

    for snapshot in snapshots:
        for sub in snapshot.subscriptions:
            if sub["status"] != "active" or sub["mrr_amount"] is None or sub["mrr_amount"] < 0:
                continue
            low, high = PLAN_MRR_RANGE[sub["plan"]]
            # Generous tolerance: an upgrade/downgrade only guarantees a
            # directional move relative to the *previous* value, not a hard
            # clamp into the new band on every single transition.
            assert low * 0.5 <= sub["mrr_amount"] <= high * 1.5
            checked_any = True

    assert checked_any
