"""Failure/status notifications.

Posts to a Slack incoming webhook when ``SLACK_WEBHOOK_URL`` is configured;
otherwise falls back to a structured log line. Either way this module must
never raise — a broken notification channel is not a reason to fail (or
further break) the pipeline it's reporting on.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

SLACK_TIMEOUT_SECONDS = 5


def _post_to_slack(text: str) -> bool:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return False
    try:
        response = requests.post(webhook_url, json={"text": text}, timeout=SLACK_TIMEOUT_SECONDS)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.warning("Slack notification failed, falling back to logs: %s", exc)
        return False


def notify_failure(context: dict[str, Any]) -> None:
    """``on_failure_callback`` for every DAG's ``default_args``.

    Never raises: called from inside Airflow's own failure-handling path, so
    an exception here would just mask the real task failure.
    """
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", context.get("dag", "unknown_dag"))
    task_id = getattr(task_instance, "task_id", "unknown_task")
    logical_date = context.get("logical_date", context.get("execution_date"))
    exception = context.get("exception")
    log_url = getattr(task_instance, "log_url", None)

    message = (
        f":rotating_light: *Task failed* — `{dag_id}.{task_id}`\n"
        f"Logical date: `{logical_date}`\n"
        f"Error: `{exception}`\n" + (f"Logs: {log_url}" if log_url else "")
    )

    try:
        if not _post_to_slack(message):
            logger.error(
                "PIPELINE_FAILURE dag_id=%s task_id=%s logical_date=%s exception=%s",
                dag_id,
                task_id,
                logical_date,
                exception,
            )
    except Exception:  # noqa: BLE001 - notification code must never raise
        logger.exception("notify_failure itself raised — swallowing to protect the pipeline.")


def notify_pipeline_status(**context: Any) -> None:
    """Transversal task (``trigger_rule="all_done"``) that reports whether
    the whole DAG run succeeded, based on the state of every other task
    instance in the run — not just whether this task itself was reached."""
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()

    failed = [ti.task_id for ti in task_instances if ti.state == "failed"]
    upstream_failed = [ti.task_id for ti in task_instances if ti.state == "upstream_failed"]

    if failed or upstream_failed:
        message = (
            f":x: *Pipeline run finished with failures* — `{dag_run.dag_id}` "
            f"(`{dag_run.logical_date}`)\n"
            f"Failed tasks: {failed}\nUpstream-failed tasks: {upstream_failed}"
        )
        if not _post_to_slack(message):
            logger.error(
                "PIPELINE_RUN_FAILED dag_id=%s logical_date=%s failed=%s upstream_failed=%s",
                dag_run.dag_id,
                dag_run.logical_date,
                failed,
                upstream_failed,
            )
    else:
        logger.info(
            "PIPELINE_RUN_SUCCEEDED dag_id=%s logical_date=%s", dag_run.dag_id, dag_run.logical_date
        )
