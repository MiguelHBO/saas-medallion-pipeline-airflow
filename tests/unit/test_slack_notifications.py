"""notify_failure must fall back to structured logging when no Slack
webhook is configured, and must never raise even if something inside it
goes wrong — it runs from inside Airflow's own failure-handling path."""

from __future__ import annotations

import logging

from include.notifications.slack import notify_failure


class _FakeTaskInstance:
    dag_id = "bronze_ingestion"
    task_id = "ingest_bronze"
    log_url = "http://localhost:8080/log"


def test_notify_failure_logs_when_no_webhook_configured(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    context = {
        "task_instance": _FakeTaskInstance(),
        "logical_date": "2026-01-01",
        "exception": RuntimeError("boom"),
    }

    with caplog.at_level(logging.ERROR):
        notify_failure(context)

    assert any("PIPELINE_FAILURE" in record.message for record in caplog.records)


def test_notify_failure_never_raises_even_with_a_malformed_context():
    notify_failure({})  # no task_instance, no exception key — must not raise
