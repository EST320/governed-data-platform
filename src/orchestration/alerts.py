"""Alerts for scheduled runs.

Messages are posted to a chat webhook (Slack- and Discord-compatible payload)
given by ALERT_WEBHOOK_URL. Without it, alerts only go to the log. An alert
that cannot be delivered is logged, never raised: a broken webhook must not
turn a successful run into a failed one, or hide the original error.

Kept free of Airflow imports so it can be tested without an Airflow install.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import requests

log = logging.getLogger("orchestration.alerts")


def send(message: str, url: str | None = None) -> bool:
    url = url if url is not None else os.environ.get("ALERT_WEBHOOK_URL", "")
    log.warning("ALERT: %s", message)
    if not url:
        return False
    try:
        # "content" is read by Discord, "text" by Slack; each ignores the other.
        r = requests.post(url, json={"content": message, "text": message}, timeout=10)
        r.raise_for_status()
        return True
    except requests.RequestException as exc:
        log.error("could not deliver alert: %s", exc)
        return False


def _get(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def failure_message(context: dict) -> str:
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    dag_id = _get(ti, "dag_id") or _get(dag_run, "dag_id", "?")
    task_id = _get(ti, "task_id", "?")
    try_number = _get(ti, "try_number", "?")
    run_id = _get(dag_run, "run_id") or context.get("run_id", "?")
    error = context.get("exception")
    return (f"[FAILED] {dag_id}.{task_id} (try {try_number}), run {run_id}: "
            f"{type(error).__name__ + ': ' + str(error) if error else 'no exception recorded'}")


def on_failure(context: dict) -> None:
    """Airflow on_failure_callback: fires once a task has used up its retries."""
    send(failure_message(context))


def deadline_message(context: dict | None, dag_id: str, target: str) -> str:
    dag_run = _get(context, "dag_run")
    run_id = _get(dag_run, "run_id", "?")
    return f"[LATE] {dag_id} run {run_id} has not finished within its target of {target}"


async def on_deadline_missed(context: dict | None = None, dag_id: str = "?", target: str = "?", **_) -> None:
    """Airflow DeadlineAlert callback (runs in the triggerer, so it must be async)."""
    await asyncio.to_thread(send, deadline_message(context, dag_id, target))
