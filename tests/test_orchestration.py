import asyncio
from datetime import date
from pathlib import Path

import pytest

from src.orchestration import alerts
from src.orchestration.schedule import previous_quarter


@pytest.mark.parametrize("run_date,expected", [
    (date(2026, 1, 5), (2025, 4)), (date(2026, 4, 5), (2026, 1)),
    (date(2026, 7, 5), (2026, 2)), (date(2026, 10, 5), (2026, 3)),
])
def test_previous_quarter(run_date, expected):
    assert previous_quarter(run_date) == expected


class TI:
    dag_id, task_id, try_number = "edgar_quarterly", "stage", 3


def test_failure_message_names_task_try_and_error():
    msg = alerts.failure_message({"task_instance": TI(), "run_id": "scheduled__2026-01-05",
                                  "exception": ValueError("bad partition")})
    assert msg == ("[FAILED] edgar_quarterly.stage (try 3), run scheduled__2026-01-05: "
                   "ValueError: bad partition")


def test_alert_is_posted_to_webhook(monkeypatch):
    sent = {}

    def fake_post(url, json, timeout):
        sent.update(url=url, json=json)
        return type("R", (), {"raise_for_status": lambda s: None})()

    monkeypatch.setattr(alerts.requests, "post", fake_post)
    assert alerts.send("hello", url="https://hooks.example/x") is True
    assert sent["json"] == {"content": "hello", "text": "hello"}


def test_undeliverable_alert_does_not_raise():
    assert alerts.send("hello", url="http://127.0.0.1:9") is False


def test_no_webhook_configured_only_logs(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert alerts.send("hello") is False


def test_deadline_callback_is_async_and_sends(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "send", lambda m, url=None: sent.append(m) or True)
    assert asyncio.iscoroutinefunction(alerts.on_deadline_missed)
    asyncio.run(alerts.on_deadline_missed({"dag_run": {"run_id": "r1"}}, dag_id="fx_rates_daily", target="0:30:00"))
    assert sent == ["[LATE] fx_rates_daily run r1 has not finished within its target of 0:30:00"]


# --- DAG structure (runs only where Airflow is installed, e.g. the airflow CI job) -----

def test_dags_import_without_errors():
    pytest.importorskip("airflow")
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert bag.import_errors == {}
    assert set(bag.dags) == {"fx_rates_daily", "edgar_quarterly"}

    edgar = bag.dags["edgar_quarterly"]
    order = {t.task_id: t.upstream_task_ids for t in edgar.tasks}
    assert order["amendments"] == {"stage"} and order["build_and_publish"] == {"amendments"}
    assert edgar.get_task("build_and_publish").retries == 0          # a failed check will not pass on retry
    assert edgar.get_task("ingest").retries == 5 and edgar.get_task("ingest").retry_exponential_backoff
    assert all(t.execution_timeout for t in edgar.tasks if t.task_id != "previous_quarter")
    assert edgar.dagrun_timeout and edgar.deadline and bag.dags["fx_rates_daily"].deadline
