"""Synthetic SaaS data generator — a local, reproducible stand-in for a real
operational system (billing + product usage).

This module is intentionally the *only* place in the project that invents
data. Everything downstream (ingestion adapters, Silver, Gold) treats its
output exactly like it would treat a real source: it doesn't know or care
that the data is synthetic.

Bronze-facing contract, one simulated day at a time:
    - ``customers``            full current snapshot as of that day
    - ``subscriptions``        full current snapshot as of that day
    - ``subscription_events``  delta: only events dated that day
    - ``invoices``              delta: only invoices issued that day
    - ``usage_events``          delta: only usage events on that day

Snapshot tables (customers, subscriptions) are cheap to re-emit in full every
day at this volume and this sidesteps change-data-capture/merge logic that
would otherwise be needed to reconstruct "what did the subscriptions table
look like on day D" from a pure delta feed — the two event-log tables
(subscription_events, invoices, usage_events) are naturally append-only, so
those *are* emitted as deltas, matching how they'd really arrive.

Modeling note on ``subscription_events``: ``created`` fires the moment a
subscription starts contributing MRR (i.e. on a direct paid signup, or when a
trial converts) rather than at signup time — trialing subscriptions contribute
$0 to MRR, so New MRR should be recognized when revenue actually starts, not
when the trial begins. A trial that never converts churns silently (no MRR
event was ever emitted, since none was ever earned).

Run standalone to (re)populate the local environment:
    python -m include.saas.data_generator --days 90

The Postgres write (simulating the "db_extract" origin) is best-effort: if
`source_db` isn't reachable (e.g. running this outside Docker, or as a test
fixture), it's skipped with a warning instead of failing the whole run — the
file-drop output is always written locally regardless.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker

from include.saas.constants import COUNTRIES, DATE_FMT, PLAN_MRR_RANGE, PLANS, USAGE_FEATURES

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INCOMING_DIR = REPO_ROOT / "include" / "data" / "incoming"

TRIAL_LENGTH_DAYS = 14

# Daily transition probabilities, applied independently per eligible
# subscription. Deliberately modest so a 90-day run produces a readable,
# still-mostly-healthy business rather than pure churn noise.
P_TRIAL_CONVERTS = 0.08
P_TRIAL_ABANDONED = 0.015
P_UPGRADE = 0.012
P_DOWNGRADE = 0.008
P_PAYMENT_FAILS = 0.02
# Slower than a real dunning cycle would resolve, on purpose: past_due needs
# to persist long enough (average ~5 days) for the Gold "at risk" flag
# (product usage decline while past_due) to have something to detect.
P_PAYMENT_RECOVERS = 0.15
P_PAST_DUE_CHURNS = 0.04

# Deliberate data-quality edge cases (spec §4.2) — kept low-rate so they read
# as "the odd bad record", not as noise that swamps the metrics.
P_ORPHAN_USAGE_EVENT = 0.01
P_PHANTOM_PAID_INVOICE = 0.01
P_DIRTY_MRR_VALUE = 0.005


@dataclass
class DailySnapshot:
    day: date
    customers: list[dict[str, Any]] = field(default_factory=list)
    subscriptions: list[dict[str, Any]] = field(default_factory=list)
    subscription_events: list[dict[str, Any]] = field(default_factory=list)
    invoices: list[dict[str, Any]] = field(default_factory=list)
    usage_events: list[dict[str, Any]] = field(default_factory=list)


def _weighted_choice(rng: random.Random, options: dict[str, float]) -> str:
    """Pick a key from ``options`` weighted by its value."""
    keys = list(options.keys())
    weights = list(options.values())
    return rng.choices(keys, weights=weights, k=1)[0]


class DataGenerator:
    """Stateful day-by-day simulator for the SaaS billing/product domain.

    Instantiate once, then call :meth:`simulate_day` for consecutive dates in
    order — state (customers, subscriptions, running sequence counters)
    carries forward between calls, the same way a real operational system
    would evolve day over day.
    """

    def __init__(self, seed: int = 42, max_customers: int = 2000) -> None:
        self._rng = random.Random(seed)
        Faker.seed(seed)
        self._faker = Faker()
        self.max_customers = max_customers

        self._customers: dict[str, dict[str, Any]] = {}
        self._subscriptions: dict[str, dict[str, Any]] = {}
        self._customer_to_subscription: dict[str, str] = {}

        self._next_customer_seq = 1
        self._next_subscription_seq = 1
        self._next_event_seq = 1
        self._next_invoice_seq = 1
        self._next_usage_seq = 1

    # -- id helpers ---------------------------------------------------

    def _new_customer_id(self) -> str:
        cid = f"cus_{self._next_customer_seq:06d}"
        self._next_customer_seq += 1
        return cid

    def _new_subscription_id(self) -> str:
        sid = f"sub_{self._next_subscription_seq:06d}"
        self._next_subscription_seq += 1
        return sid

    def _new_event_id(self) -> str:
        eid = f"evt_{self._next_event_seq:07d}"
        self._next_event_seq += 1
        return eid

    def _new_invoice_id(self) -> str:
        iid = f"inv_{self._next_invoice_seq:07d}"
        self._next_invoice_seq += 1
        return iid

    def _new_usage_event_id(self) -> str:
        uid = f"use_{self._next_usage_seq:08d}"
        self._next_usage_seq += 1
        return uid

    def _random_mrr(self, plan: str) -> float:
        low, high = PLAN_MRR_RANGE[plan]
        return round(self._rng.uniform(low, high), 2)

    def _maybe_dirty(self, value: float) -> float | None:
        """Occasionally corrupt an MRR value to exercise data-quality checks."""
        if self._rng.random() < P_DIRTY_MRR_VALUE:
            return self._rng.choice([None, -abs(value)])
        return value

    # -- signups --------------------------------------------------------

    def _signups_today(self, day_index: int, day: date) -> int:
        if len(self._customers) >= self.max_customers:
            return 0
        base = 10.0 * (1.0 + day_index * 0.01)
        if day.weekday() >= 5:  # weekend seasonality
            base *= 0.4
        n = int(round(self._rng.gauss(base, base * 0.25)))
        return max(0, min(n, self.max_customers - len(self._customers)))

    def _create_customer(self, day: date) -> dict[str, Any]:
        customer_id = self._new_customer_id()
        record = {
            "customer_id": customer_id,
            "company_name": self._faker.unique.company(),
            "segment": _weighted_choice(
                self._rng, {"SMB": 0.6, "Mid-Market": 0.3, "Enterprise": 0.1}
            ),
            "country": self._rng.choice(COUNTRIES),
            "signup_date": day.strftime(DATE_FMT),
        }
        self._customers[customer_id] = record
        return record

    def _create_subscription(self, customer: dict[str, Any], day: date) -> dict[str, Any]:
        plan = _weighted_choice(
            self._rng, {"Starter": 0.5, "Growth": 0.35, "Scale": 0.12, "Enterprise": 0.03}
        )
        starts_trialing = self._rng.random() < 0.7
        subscription_id = self._new_subscription_id()
        record = {
            "subscription_id": subscription_id,
            "customer_id": customer["customer_id"],
            "plan": plan,
            "mrr_amount": 0.0 if starts_trialing else self._random_mrr(plan),
            "status": "trialing" if starts_trialing else "active",
            "start_date": day.strftime(DATE_FMT),
            "end_date": None,
            "billing_cycle": _weighted_choice(self._rng, {"monthly": 0.8, "annual": 0.2}),
            "trial_ends_on": (
                (day + timedelta(days=TRIAL_LENGTH_DAYS)).strftime(DATE_FMT)
                if starts_trialing
                else None
            ),
            "updated_at": day.strftime(DATE_FMT),
        }
        self._subscriptions[subscription_id] = record
        self._customer_to_subscription[customer["customer_id"]] = subscription_id
        return record

    # -- subscription lifecycle -----------------------------------------

    def _emit_event(
        self,
        subscription_id: str,
        event_type: str,
        day: date,
        mrr_before: float | None,
        mrr_after: float | None,
    ) -> dict[str, Any]:
        return {
            "event_id": self._new_event_id(),
            "subscription_id": subscription_id,
            "event_type": event_type,
            "event_date": day.strftime(DATE_FMT),
            "mrr_before": mrr_before,
            "mrr_after": mrr_after,
        }

    def _progress_subscription(
        self, sub: dict[str, Any], day: date, events_out: list[dict[str, Any]]
    ) -> None:
        status = sub["status"]
        roll = self._rng.random()

        if status == "trialing":
            trial_ends_on = sub.get("trial_ends_on")
            if trial_ends_on is not None and day.strftime(DATE_FMT) >= trial_ends_on:
                if roll < P_TRIAL_CONVERTS / max(P_TRIAL_CONVERTS + P_TRIAL_ABANDONED, 1e-9):
                    new_mrr = self._maybe_dirty(self._random_mrr(sub["plan"]))
                    events_out.append(
                        self._emit_event(sub["subscription_id"], "created", day, 0.0, new_mrr)
                    )
                    sub["mrr_amount"] = new_mrr if new_mrr is not None else 0.0
                    sub["status"] = "active"
                else:
                    events_out.append(
                        self._emit_event(sub["subscription_id"], "canceled", day, 0.0, 0.0)
                    )
                    sub["status"] = "canceled"
                    sub["end_date"] = day.strftime(DATE_FMT)
                sub["updated_at"] = day.strftime(DATE_FMT)
            return

        if status in ("active", "past_due"):
            current_idx = PLANS.index(sub["plan"])
            can_upgrade = current_idx < len(PLANS) - 1
            can_downgrade = current_idx > 0

            if roll < P_UPGRADE and can_upgrade:
                target_plan = PLANS[current_idx + 1]
                before = sub["mrr_amount"]
                # Land within the new plan's own band, but always a genuine increase.
                after = self._maybe_dirty(max(self._random_mrr(target_plan), before * 1.05))
                events_out.append(
                    self._emit_event(sub["subscription_id"], "upgraded", day, before, after)
                )
                sub["plan"] = target_plan
                sub["mrr_amount"] = after if after is not None else before
                sub["updated_at"] = day.strftime(DATE_FMT)
            elif roll < P_UPGRADE + P_DOWNGRADE and can_downgrade:
                target_plan = PLANS[current_idx - 1]
                before = sub["mrr_amount"]
                # Land within the new plan's own band, but always a genuine decrease.
                after = self._maybe_dirty(min(self._random_mrr(target_plan), before * 0.95))
                events_out.append(
                    self._emit_event(sub["subscription_id"], "downgraded", day, before, after)
                )
                sub["plan"] = target_plan
                sub["mrr_amount"] = after if after is not None else before
                sub["updated_at"] = day.strftime(DATE_FMT)
            elif status == "active" and roll < P_UPGRADE + P_DOWNGRADE + P_PAYMENT_FAILS:
                sub["status"] = "past_due"
                sub["updated_at"] = day.strftime(DATE_FMT)
            elif status == "past_due":
                if roll < P_PAST_DUE_CHURNS:
                    before = sub["mrr_amount"]
                    events_out.append(
                        self._emit_event(sub["subscription_id"], "canceled", day, before, 0.0)
                    )
                    sub["status"] = "canceled"
                    sub["end_date"] = day.strftime(DATE_FMT)
                    sub["updated_at"] = day.strftime(DATE_FMT)
                elif roll < P_PAST_DUE_CHURNS + P_PAYMENT_RECOVERS:
                    sub["status"] = "active"
                    sub["updated_at"] = day.strftime(DATE_FMT)
            return

        # Rare win-back: a canceled customer reactivates.
        if status == "canceled" and roll < 0.01:
            before = 0.0
            after = self._maybe_dirty(self._random_mrr(sub["plan"]))
            events_out.append(
                self._emit_event(sub["subscription_id"], "reactivated", day, before, after)
            )
            sub["status"] = "active"
            sub["mrr_amount"] = after if after is not None else self._random_mrr(sub["plan"])
            sub["end_date"] = None
            sub["updated_at"] = day.strftime(DATE_FMT)

    # -- invoices ---------------------------------------------------------

    def _is_billing_due(self, sub: dict[str, Any], day: date) -> bool:
        start = datetime.strptime(sub["start_date"], DATE_FMT).date()
        elapsed = (day - start).days
        if elapsed < 0:
            return False
        cycle_days = 365 if sub["billing_cycle"] == "annual" else 30
        return elapsed > 0 and elapsed % cycle_days == 0

    def _generate_invoices(self, day: date) -> list[dict[str, Any]]:
        invoices: list[dict[str, Any]] = []
        for sub in self._subscriptions.values():
            if sub["status"] not in ("active", "past_due"):
                continue
            if not self._is_billing_due(sub, day):
                continue
            amount = sub["mrr_amount"] * (12 if sub["billing_cycle"] == "annual" else 1)
            status = _weighted_choice(self._rng, {"paid": 0.90, "failed": 0.07, "pending": 0.03})
            issued_date = day.strftime(DATE_FMT)
            paid_date = (
                (day + timedelta(days=self._rng.randint(0, 3))).strftime(DATE_FMT)
                if status == "paid"
                else None
            )
            invoices.append(
                {
                    "invoice_id": self._new_invoice_id(),
                    "customer_id": sub["customer_id"],
                    "subscription_id": sub["subscription_id"],
                    "amount": round(amount, 2),
                    "status": status,
                    "issued_date": issued_date,
                    "paid_date": paid_date,
                }
            )

        # Deliberate inconsistency: a "paid" invoice for a subscription that
        # is not actually active — this is what Silver's reconciliation
        # check (spec §6.3) is supposed to catch.
        canceled = [s for s in self._subscriptions.values() if s["status"] == "canceled"]
        if canceled and self._rng.random() < P_PHANTOM_PAID_INVOICE:
            ghost_sub = self._rng.choice(canceled)
            invoices.append(
                {
                    "invoice_id": self._new_invoice_id(),
                    "customer_id": ghost_sub["customer_id"],
                    "subscription_id": ghost_sub["subscription_id"],
                    "amount": self._random_mrr(ghost_sub["plan"]),
                    "status": "paid",
                    "issued_date": day.strftime(DATE_FMT),
                    "paid_date": day.strftime(DATE_FMT),
                }
            )
        return invoices

    # -- usage events -----------------------------------------------------

    def _generate_usage_events(self, day: date) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        eligible = [
            s for s in self._subscriptions.values() if s["status"] in ("active", "past_due")
        ]
        for sub in eligible:
            customer = self._customers[sub["customer_id"]]
            n_seats = {"SMB": 3, "Mid-Market": 10, "Enterprise": 30}[customer["segment"]]
            # A failed payment doesn't happen in a vacuum — accounts that
            # go past_due are, on average, already disengaging. Modeling
            # that drop here is what makes Gold's "at risk" flag (usage
            # decline + past_due) detectable at all instead of dead logic.
            max_events = n_seats * 2 if sub["status"] == "active" else max(1, n_seats // 3)
            n_events = self._rng.randint(0, max_events)
            for _ in range(n_events):
                user_num = self._rng.randint(1, n_seats)
                ts = datetime.combine(day, datetime.min.time()) + timedelta(
                    seconds=self._rng.randint(0, 86399)
                )
                events.append(
                    {
                        "event_id": self._new_usage_event_id(),
                        "customer_id": sub["customer_id"],
                        "user_id": f"{sub['customer_id']}_user_{user_num:03d}",
                        "feature": self._rng.choice(USAGE_FEATURES),
                        "event_timestamp": ts.isoformat(),
                    }
                )

        # Deliberate orphan: a usage event for a customer_id that doesn't exist.
        if self._rng.random() < P_ORPHAN_USAGE_EVENT:
            ghost_customer_id = f"cus_ghost_{self._rng.randint(1, 9999):04d}"
            ts = datetime.combine(day, datetime.min.time()) + timedelta(
                seconds=self._rng.randint(0, 86399)
            )
            events.append(
                {
                    "event_id": self._new_usage_event_id(),
                    "customer_id": ghost_customer_id,
                    "user_id": f"{ghost_customer_id}_user_001",
                    "feature": self._rng.choice(USAGE_FEATURES),
                    "event_timestamp": ts.isoformat(),
                }
            )
        return events

    # -- public API ---------------------------------------------------------

    def simulate_day(self, day: date, day_index: int) -> DailySnapshot:
        """Advance the simulation by one day and return that day's Bronze payload."""
        subscription_events: list[dict[str, Any]] = []

        for _ in range(self._signups_today(day_index, day)):
            customer = self._create_customer(day)
            self._create_subscription(customer, day)

        for sub in list(self._subscriptions.values()):
            self._progress_subscription(sub, day, subscription_events)

        return DailySnapshot(
            day=day,
            customers=list(self._customers.values()),
            subscriptions=list(self._subscriptions.values()),
            subscription_events=subscription_events,
            invoices=self._generate_invoices(day),
            usage_events=self._generate_usage_events(day),
        )

    def run(self, days: int, start_date: date) -> list[DailySnapshot]:
        """Simulate ``days`` consecutive days starting at ``start_date``."""
        snapshots = []
        for i in range(days):
            day = start_date + timedelta(days=i)
            snapshots.append(self.simulate_day(day, i))
        return snapshots


# -- persistence ------------------------------------------------------------


def write_snapshot_to_incoming(
    snapshot: DailySnapshot, base_dir: Path = DEFAULT_INCOMING_DIR
) -> Path:
    """Write one day's Bronze payload as JSON files under ``base_dir/<day>/``.

    This simulates the "file drop" origin (spec §4.3 #1): a folder someone —
    or something — deposits the day's export into.
    """
    day_dir = base_dir / snapshot.day.strftime(DATE_FMT)
    day_dir.mkdir(parents=True, exist_ok=True)

    payloads = {
        "customers": snapshot.customers,
        "subscriptions": snapshot.subscriptions,
        "subscription_events": snapshot.subscription_events,
        "invoices": snapshot.invoices,
        "usage_events": snapshot.usage_events,
    }
    for name, records in payloads.items():
        with open(day_dir / f"{name}.json", "w", encoding="utf-8") as fh:
            json.dump(records, fh, default=str)
    return day_dir


def write_snapshot_to_source_db(snapshot: DailySnapshot, connection_uri: str | None = None) -> bool:
    """Best-effort append of one day's payload into the "source system" DB.

    Simulates the "db extract" origin (spec §4.3 #2). Returns True if the
    write succeeded, False if it was skipped (e.g. DB unreachable) — the
    caller should treat False as a warning, never a fatal error, since the
    generator must keep working with no Docker/DB available at all.

    ``customers``/``subscriptions`` are tagged with ``snapshot_date`` and
    appended (not replaced) every day, standing in for a source system that
    supports as-of queries — the real alternative to a live OLTP table that
    only ever shows "now" (which would make a historical backfill from this
    origin meaningless: you can't ask a plain OLTP table what it looked like
    60 days ago). db_extract.py filters on ``snapshot_date`` and drops the
    column again before handing off to Bronze, so both origins produce the
    exact same shape.
    """
    import pandas as pd

    try:
        from sqlalchemy import create_engine
    except ImportError:
        logger.warning("SQLAlchemy not installed — skipping source_db write.")
        return False

    uri = connection_uri or os.environ.get("SOURCE_DB_SQLALCHEMY_URI")
    if not uri:
        user = os.environ.get("SOURCE_DB_USER", "saas_app")
        password = os.environ.get("SOURCE_DB_PASSWORD", "saas_app")
        host = os.environ.get("SOURCE_DB_HOST", "localhost")
        port = os.environ.get("SOURCE_DB_PORT", "5433")
        name = os.environ.get("SOURCE_DB_NAME", "saas_source")
        uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"

    try:
        engine = create_engine(uri, connect_args={"connect_timeout": 3})
        with engine.connect():
            pass
    except Exception as exc:  # pragma: no cover - environment-dependent
        logger.warning("source_db unreachable (%s) — skipping DB write for %s.", exc, snapshot.day)
        return False

    # Snapshot tables, tagged with the day they represent and appended so
    # every historical day stays queryable (see docstring above).
    snapshot_tag = snapshot.day.strftime(DATE_FMT)
    customers_df = pd.DataFrame(snapshot.customers)
    customers_df["snapshot_date"] = snapshot_tag
    customers_df.to_sql("customers", engine, if_exists="append", index=False)

    subscriptions_df = pd.DataFrame(snapshot.subscriptions)
    subscriptions_df["snapshot_date"] = snapshot_tag
    subscriptions_df.to_sql("subscriptions", engine, if_exists="append", index=False)

    # Append-only event logs.
    if snapshot.subscription_events:
        pd.DataFrame(snapshot.subscription_events).to_sql(
            "subscription_events", engine, if_exists="append", index=False
        )
    if snapshot.invoices:
        pd.DataFrame(snapshot.invoices).to_sql("invoices", engine, if_exists="append", index=False)
    if snapshot.usage_events:
        pd.DataFrame(snapshot.usage_events).to_sql(
            "usage_events", engine, if_exists="append", index=False
        )
    engine.dispose()
    return True


def generate_and_persist(
    days: int,
    start_date: date | None = None,
    seed: int = 42,
    max_customers: int = 2000,
    skip_db: bool = False,
    base_dir: Path = DEFAULT_INCOMING_DIR,
) -> list[DailySnapshot]:
    """Run the full simulation and write every day's output to disk (and,
    best-effort, to the source DB)."""
    start = start_date or (date.today() - timedelta(days=days - 1))
    generator = DataGenerator(seed=seed, max_customers=max_customers)
    snapshots = generator.run(days=days, start_date=start)

    db_writes_ok = 0
    for snapshot in snapshots:
        write_snapshot_to_incoming(snapshot, base_dir=base_dir)
        if not skip_db and write_snapshot_to_source_db(snapshot):
            db_writes_ok += 1

    logger.info(
        "Generated %d day(s) starting %s: %d customers, %d subscriptions, "
        "%d subscription_events, %d invoices, %d usage_events total. "
        "source_db writes succeeded for %d/%d day(s).",
        len(snapshots),
        start.strftime(DATE_FMT),
        len(snapshots[-1].customers) if snapshots else 0,
        len(snapshots[-1].subscriptions) if snapshots else 0,
        sum(len(s.subscription_events) for s in snapshots),
        sum(len(s.invoices) for s in snapshots),
        sum(len(s.usage_events) for s in snapshots),
        db_writes_ok,
        0 if skip_db else len(snapshots),
    )
    return snapshots


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=int(os.environ.get("GENERATOR_DEFAULT_DAYS", 90)),
        help="Number of consecutive days to simulate (default: env GENERATOR_DEFAULT_DAYS or 90).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="First simulated day, YYYY-MM-DD (default: today minus --days, so the run ends today).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("GENERATOR_SEED", 42)),
        help="RNG seed for reproducibility (default: env GENERATOR_SEED or 42).",
    )
    parser.add_argument("--max-customers", type=int, default=2000)
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Only write the file-drop output; don't attempt the source_db write.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    start = datetime.strptime(args.start_date, DATE_FMT).date() if args.start_date else None
    generate_and_persist(
        days=args.days,
        start_date=start,
        seed=args.seed,
        max_customers=args.max_customers,
        skip_db=args.skip_db,
    )


if __name__ == "__main__":
    main()
