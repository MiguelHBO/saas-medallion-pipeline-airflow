"""Gold layer: ``saas_metrics_daily`` (MRR/ARR/churn/NRR) and
``product_engagement_daily`` (DAU/WAU/MAU + an at-risk flag).

The MRR-waterfall math is kept as pure functions over plain DataFrames (no
Parquet, no Airflow) specifically so it's directly unit-testable against a
fixed, hand-computed set of ``subscription_events`` — this is the part of
the pipeline most likely to get probed in an interview, so it needs to be
the easiest part to point at and reason about in isolation.

Modeling choice: a ``reactivated`` event is folded into **New MRR**, not its
own bucket — a reactivation's MRR baseline at the start of the day was zero,
the same starting point as a brand-new subscription. Spec's NRR formula only
has four buckets (new/expansion/contraction/churned); this is the simplest
way to be consistent with that without inventing a fifth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from include.saas.constants import DATE_FMT, MRR_ELIGIBLE_STATUSES
from include.saas.transform import SILVER_DIR, read_silver_table

REPO_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = REPO_ROOT / "include" / "data" / "gold"

ENGAGEMENT_WINDOW_DAYS = 30
# Shorter than the "ex: last 14 days" in the spec on purpose: in this model
# past_due only persists a few days on average (see data_generator.py) before
# resolving one way or the other, so a 14+14-day comparison window would
# mostly compare two periods where the account was already active again,
# diluting the signal it's meant to catch.
AT_RISK_LOOKBACK_DAYS = 7
RECONCILIATION_TOLERANCE = 0.01  # USD; guards against float rounding only


def _eligible(subscriptions: pd.DataFrame) -> pd.DataFrame:
    if subscriptions.empty:
        return subscriptions
    return subscriptions[subscriptions["status"].isin(MRR_ELIGIBLE_STATUSES)]


def compute_mrr_waterfall(subscription_events_today: pd.DataFrame) -> dict[str, float]:
    """New/Expansion/Contraction/Churned MRR from one day's subscription_events."""
    if subscription_events_today.empty:
        return {"new_mrr": 0.0, "expansion_mrr": 0.0, "contraction_mrr": 0.0, "churned_mrr": 0.0}

    events = subscription_events_today

    def _delta_sum(event_types: list[str], before_col: str, after_col: str) -> float:
        subset = events[events["event_type"].isin(event_types)]
        if subset.empty:
            return 0.0
        return round(float((subset[after_col] - subset[before_col]).sum()), 2)

    return {
        "new_mrr": _delta_sum(["created", "reactivated"], "mrr_before", "mrr_after"),
        "expansion_mrr": _delta_sum(["upgraded"], "mrr_before", "mrr_after"),
        "contraction_mrr": _delta_sum(["downgraded"], "mrr_after", "mrr_before"),
        "churned_mrr": _delta_sum(["canceled"], "mrr_after", "mrr_before"),
    }


@dataclass
class GoldMetricsInputs:
    execution_date: date
    subscriptions_today: pd.DataFrame
    subscriptions_yesterday: pd.DataFrame | None
    subscription_events_today: pd.DataFrame


def compute_saas_metrics_daily(inputs: GoldMetricsInputs) -> dict:
    """One row of ``saas_metrics_daily`` for ``inputs.execution_date``.

    Churn rate, revenue-churn rate, and NRR all need a start-of-period
    baseline — yesterday's subscriptions snapshot. On the very first day of
    the whole history (no prior snapshot available), those three come back
    as ``None`` rather than a misleading 0/0.
    """
    today_eligible = _eligible(inputs.subscriptions_today)
    mrr_total = (
        round(float(today_eligible["mrr_amount"].sum()), 2) if not today_eligible.empty else 0.0
    )
    customers_active = (
        int(today_eligible["customer_id"].nunique()) if not today_eligible.empty else 0
    )

    if inputs.subscriptions_yesterday is not None:
        prior_eligible = _eligible(inputs.subscriptions_yesterday)
        mrr_start = (
            round(float(prior_eligible["mrr_amount"].sum()), 2) if not prior_eligible.empty else 0.0
        )
        customers_start = (
            int(prior_eligible["customer_id"].nunique()) if not prior_eligible.empty else 0
        )
    else:
        mrr_start = None
        customers_start = None

    waterfall = compute_mrr_waterfall(inputs.subscription_events_today)

    events = inputs.subscription_events_today
    real_churn = (
        events[(events["event_type"] == "canceled") & (events["mrr_before"] > 0)]
        if not events.empty
        else events
    )
    churned_customers_count = (
        int(real_churn["subscription_id"].nunique()) if not real_churn.empty else 0
    )

    customer_churn_rate = (
        round(churned_customers_count / customers_start, 4) if customers_start else None
    )
    revenue_churn_rate = round(waterfall["churned_mrr"] / mrr_start, 4) if mrr_start else None
    nrr = (
        round(
            (
                mrr_start
                + waterfall["expansion_mrr"]
                - waterfall["contraction_mrr"]
                - waterfall["churned_mrr"]
            )
            / mrr_start,
            4,
        )
        if mrr_start
        else None
    )
    arpu = round(mrr_total / customers_active, 2) if customers_active else None

    return {
        "dt": inputs.execution_date.strftime(DATE_FMT),
        "mrr_total": mrr_total,
        "arr_total": round(mrr_total * 12, 2),
        "new_mrr": waterfall["new_mrr"],
        "expansion_mrr": waterfall["expansion_mrr"],
        "contraction_mrr": waterfall["contraction_mrr"],
        "churned_mrr": waterfall["churned_mrr"],
        "customer_churn_rate": customer_churn_rate,
        "revenue_churn_rate": revenue_churn_rate,
        "nrr": nrr,
        "arpu": arpu,
        "customers_active": customers_active,
        "customers_start_of_period": customers_start,
    }


def compute_product_engagement_daily(
    execution_date: date,
    usage_events_window: pd.DataFrame,
    subscriptions_today: pd.DataFrame,
) -> pd.DataFrame:
    """One row per customer_id: DAU/WAU/MAU flags for ``execution_date`` plus
    an "at risk" flag (usage roughly halved over the last ``AT_RISK_LOOKBACK_DAYS``
    days *and* the subscription is currently past_due — a simple example of
    reasoning across billing and product data together, not just mechanical
    aggregation)."""
    known_customers = (
        subscriptions_today["customer_id"].unique() if not subscriptions_today.empty else []
    )

    if usage_events_window.empty:
        customer_ids = pd.Index(known_customers).unique()
        dau_ids = wau_ids = mau_ids = set()
        recent_counts = prior_counts = pd.Series(dtype=int)
    else:
        window = usage_events_window.copy()
        window["event_date"] = pd.to_datetime(window["event_timestamp"]).dt.date

        dau_ids = set(window.loc[window["event_date"] == execution_date, "customer_id"])
        wau_ids = set(
            window.loc[window["event_date"] >= execution_date - timedelta(days=6), "customer_id"]
        )
        mau_ids = set(
            window.loc[
                window["event_date"] >= execution_date - timedelta(days=ENGAGEMENT_WINDOW_DAYS - 1),
                "customer_id",
            ]
        )

        recent_start = execution_date - timedelta(days=AT_RISK_LOOKBACK_DAYS - 1)
        prior_start = execution_date - timedelta(days=2 * AT_RISK_LOOKBACK_DAYS - 1)
        prior_end = execution_date - timedelta(days=AT_RISK_LOOKBACK_DAYS)
        recent_counts = window[window["event_date"] >= recent_start].groupby("customer_id").size()
        prior_counts = (
            window[(window["event_date"] >= prior_start) & (window["event_date"] <= prior_end)]
            .groupby("customer_id")
            .size()
        )
        customer_ids = pd.Index(window["customer_id"]).union(pd.Index(known_customers)).unique()

    status_by_customer = (
        subscriptions_today.set_index("customer_id")["status"]
        if not subscriptions_today.empty
        else pd.Series(dtype=str)
    )

    rows = []
    for customer_id in customer_ids:
        recent = int(recent_counts.get(customer_id, 0))
        prior = int(prior_counts.get(customer_id, 0))
        usage_dropped = prior > 0 and recent < prior * 0.5
        status = status_by_customer.get(customer_id)
        rows.append(
            {
                "dt": execution_date.strftime(DATE_FMT),
                "customer_id": customer_id,
                "dau": int(customer_id in dau_ids),
                "wau": int(customer_id in wau_ids),
                "mau": int(customer_id in mau_ids),
                "at_risk": bool(status == "past_due" and usage_dropped),
            }
        )
    return pd.DataFrame(rows)


def assert_mrr_reconciliation(subscriptions_today: pd.DataFrame, saas_metrics_row: dict) -> None:
    """The end-to-end check spec §6.4 asks for: Gold's ``mrr_total`` must
    equal the sum of ``mrr_amount`` over Silver's active/past_due
    subscriptions for the same day. Raises ``AssertionError`` if not."""
    expected = (
        round(float(_eligible(subscriptions_today)["mrr_amount"].sum()), 2)
        if not subscriptions_today.empty
        else 0.0
    )
    actual = saas_metrics_row["mrr_total"]
    if abs(expected - actual) > RECONCILIATION_TOLERANCE:
        raise AssertionError(
            f"MRR reconciliation failed for {saas_metrics_row['dt']}: "
            f"Silver sum={expected} vs Gold mrr_total={actual}"
        )


# -- I/O: reading Silver, writing Gold ---------------------------------------


def _read_silver_or_empty(entity: str, day: date) -> pd.DataFrame:
    try:
        return read_silver_table(entity, day, base_dir=SILVER_DIR)
    except FileNotFoundError:
        return pd.DataFrame()


def load_gold_inputs(execution_date: date) -> tuple[GoldMetricsInputs, pd.DataFrame]:
    """Read everything :func:`compute_saas_metrics_daily` and
    :func:`compute_product_engagement_daily` need for one day, from Silver."""
    subscriptions_today = _read_silver_or_empty("subscriptions", execution_date)
    subscriptions_yesterday_df = _read_silver_or_empty(
        "subscriptions", execution_date - timedelta(days=1)
    )
    subscriptions_yesterday = (
        subscriptions_yesterday_df if not subscriptions_yesterday_df.empty else None
    )
    subscription_events_today = _read_silver_or_empty("subscription_events", execution_date)

    usage_frames = [
        _read_silver_or_empty("usage_events", execution_date - timedelta(days=offset))
        for offset in range(ENGAGEMENT_WINDOW_DAYS)
    ]
    usage_frames = [f for f in usage_frames if not f.empty]
    usage_events_window = (
        pd.concat(usage_frames, ignore_index=True) if usage_frames else pd.DataFrame()
    )

    metrics_inputs = GoldMetricsInputs(
        execution_date=execution_date,
        subscriptions_today=subscriptions_today,
        subscriptions_yesterday=subscriptions_yesterday,
        subscription_events_today=subscription_events_today,
    )
    return metrics_inputs, usage_events_window


def persist_gold(
    execution_date: date,
    saas_metrics_row: dict,
    engagement_df: pd.DataFrame,
    base_dir: Path = GOLD_DIR,
) -> dict[str, Path]:
    dt_str = execution_date.strftime(DATE_FMT)

    metrics_dir = base_dir / "saas_metrics_daily" / f"dt={dt_str}"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / "part-0.parquet"
    pd.DataFrame([saas_metrics_row]).to_parquet(metrics_path, index=False)

    engagement_dir = base_dir / "product_engagement_daily" / f"dt={dt_str}"
    engagement_dir.mkdir(parents=True, exist_ok=True)
    engagement_path = engagement_dir / "part-0.parquet"
    engagement_df.to_parquet(engagement_path, index=False)

    return {"saas_metrics_daily": metrics_path, "product_engagement_daily": engagement_path}


def read_gold_metrics_row(execution_date: date, base_dir: Path = GOLD_DIR) -> dict:
    """Re-read the persisted ``saas_metrics_daily`` row for one day — used by
    the Gold DAG's validation task to reconcile against what's actually on
    disk, not whatever the compute task happened to hold in memory."""
    path = (
        base_dir
        / "saas_metrics_daily"
        / f"dt={execution_date.strftime(DATE_FMT)}"
        / "part-0.parquet"
    )
    if not path.exists():
        raise FileNotFoundError(
            f"No Gold saas_metrics_daily partition for {execution_date} at {path}"
        )
    return pd.read_parquet(path).iloc[0].to_dict()


def compute_and_persist_gold(execution_date: date) -> dict:
    """Full Gold task body for one day: read Silver, compute both tables,
    reconcile, persist. Returns the ``saas_metrics_daily`` row (handy for
    logging/XCom)."""
    inputs, usage_events_window = load_gold_inputs(execution_date)
    saas_metrics_row = compute_saas_metrics_daily(inputs)
    engagement_df = compute_product_engagement_daily(
        execution_date, usage_events_window, inputs.subscriptions_today
    )
    assert_mrr_reconciliation(inputs.subscriptions_today, saas_metrics_row)
    # Pass base_dir explicitly (looked up here, at call time) rather than
    # relying on persist_gold's own default — a default argument value is
    # bound once when persist_gold is *defined*, so a test monkeypatching
    # this module's GOLD_DIR wouldn't otherwise reach it.
    persist_gold(execution_date, saas_metrics_row, engagement_df, base_dir=GOLD_DIR)
    return saas_metrics_row
