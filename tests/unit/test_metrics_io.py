"""Gold I/O round-trip: reading Silver partitions off disk and persisting/
re-reading Gold output — exercised against a tmp_path, not the real
include/data tree."""

from __future__ import annotations

from datetime import date

import pandas as pd

import include.saas.metrics as metrics_module
from include.saas.metrics import (
    compute_and_persist_gold,
    load_gold_inputs,
    persist_gold,
    read_gold_metrics_row,
)
from include.saas.transform import SilverResult, persist


def _write_silver_day(base_dir, day: date, mrr_amount: float) -> None:
    tables = {
        "customers": pd.DataFrame([{"customer_id": "cus_1", "signup_date": pd.Timestamp(day)}]),
        "subscriptions": pd.DataFrame(
            [
                {
                    "subscription_id": "sub_1",
                    "customer_id": "cus_1",
                    "plan": "Starter",
                    "mrr_amount": mrr_amount,
                    "status": "active",
                }
            ]
        ),
        "subscription_events": pd.DataFrame(
            columns=[
                "event_id",
                "subscription_id",
                "event_type",
                "event_date",
                "mrr_before",
                "mrr_after",
            ]
        ),
        "invoices": pd.DataFrame(
            columns=["invoice_id", "customer_id", "subscription_id", "status"]
        ),
        "usage_events": pd.DataFrame(
            [{"customer_id": "cus_1", "event_timestamp": pd.Timestamp(day) + pd.Timedelta(hours=9)}]
        ),
    }
    result = SilverResult(
        execution_date=day,
        tables=tables,
        reports=[],
        reconciliation_issues=pd.DataFrame(),
        orphan_usage_events=pd.DataFrame(),
    )
    persist(result, base_dir=base_dir)


def test_load_gold_inputs_reads_today_and_yesterday_from_silver(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver"
    monkeypatch.setattr(metrics_module, "SILVER_DIR", silver_dir)

    _write_silver_day(silver_dir, date(2026, 2, 1), mrr_amount=100.0)
    _write_silver_day(silver_dir, date(2026, 2, 2), mrr_amount=150.0)

    inputs, usage_window = load_gold_inputs(date(2026, 2, 2))

    assert inputs.subscriptions_today.iloc[0]["mrr_amount"] == 150.0
    assert inputs.subscriptions_yesterday is not None
    assert inputs.subscriptions_yesterday.iloc[0]["mrr_amount"] == 100.0
    assert len(usage_window) >= 1


def test_load_gold_inputs_without_prior_day_returns_none_baseline(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver"
    monkeypatch.setattr(metrics_module, "SILVER_DIR", silver_dir)

    _write_silver_day(silver_dir, date(2026, 2, 1), mrr_amount=100.0)

    inputs, _usage_window = load_gold_inputs(date(2026, 2, 1))

    assert inputs.subscriptions_yesterday is None


def test_persist_and_read_gold_metrics_row_roundtrip(tmp_path):
    gold_dir = tmp_path / "gold"
    row = {"dt": "2026-02-02", "mrr_total": 150.0, "arr_total": 1800.0}
    persist_gold(
        date(2026, 2, 2), row, pd.DataFrame([{"customer_id": "cus_1", "dau": 1}]), base_dir=gold_dir
    )

    reread = read_gold_metrics_row(date(2026, 2, 2), base_dir=gold_dir)
    assert reread["mrr_total"] == 150.0


def test_compute_and_persist_gold_end_to_end(tmp_path, monkeypatch):
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    monkeypatch.setattr(metrics_module, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(metrics_module, "GOLD_DIR", gold_dir)

    _write_silver_day(silver_dir, date(2026, 2, 1), mrr_amount=100.0)
    _write_silver_day(silver_dir, date(2026, 2, 2), mrr_amount=150.0)

    row = compute_and_persist_gold(date(2026, 2, 2))

    assert row["mrr_total"] == 150.0
    reread = read_gold_metrics_row(date(2026, 2, 2), base_dir=gold_dir)
    assert reread["mrr_total"] == 150.0
