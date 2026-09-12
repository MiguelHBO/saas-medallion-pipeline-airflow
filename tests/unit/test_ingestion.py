"""FileDropAdapter tests. DbExtractAdapter needs a live Airflow Connection +
Postgres and is exercised via the DAG-level verification instead (see
tests/dags and the README's manual verification notes)."""

from __future__ import annotations

import json
from datetime import date

import pytest

from include.saas.ingestion import persist_bronze, read_bronze
from include.saas.ingestion.file_drop import FileDropAdapter


def test_file_drop_adapter_reads_all_entities(tmp_path):
    day_dir = tmp_path / "2026-01-01"
    day_dir.mkdir()
    (day_dir / "customers.json").write_text(json.dumps([{"customer_id": "cus_1"}]))
    (day_dir / "subscriptions.json").write_text(json.dumps([{"subscription_id": "sub_1"}]))
    (day_dir / "subscription_events.json").write_text("[]")
    (day_dir / "invoices.json").write_text("[]")
    (day_dir / "usage_events.json").write_text("[]")

    payload = FileDropAdapter(incoming_dir=tmp_path).extract(date(2026, 1, 1))

    assert payload["customers"] == [{"customer_id": "cus_1"}]
    assert payload["subscriptions"] == [{"subscription_id": "sub_1"}]
    assert payload["invoices"] == []


def test_file_drop_adapter_raises_when_day_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        FileDropAdapter(incoming_dir=tmp_path).extract(date(2026, 1, 1))


def test_file_drop_adapter_treats_missing_entity_file_as_empty(tmp_path):
    day_dir = tmp_path / "2026-01-01"
    day_dir.mkdir()
    (day_dir / "customers.json").write_text(json.dumps([{"customer_id": "cus_1"}]))
    # every other entity file intentionally absent

    payload = FileDropAdapter(incoming_dir=tmp_path).extract(date(2026, 1, 1))

    assert payload["customers"] == [{"customer_id": "cus_1"}]
    assert payload["usage_events"] == []


def test_persist_and_read_bronze_roundtrip(tmp_path):
    payload = {
        "customers": [{"customer_id": "cus_1"}],
        "subscriptions": [{"subscription_id": "sub_1"}],
        "subscription_events": [],
        "invoices": [],
        "usage_events": [],
    }
    persist_bronze(date(2026, 1, 1), payload, base_dir=tmp_path)
    reread = read_bronze(date(2026, 1, 1), base_dir=tmp_path)

    assert reread == payload


def test_read_bronze_raises_when_partition_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_bronze(date(2026, 1, 1), base_dir=tmp_path)
