"""Airflow DAGs (Airflow 3.1+).

fx_rates_daily        weekdays 17:00 Frankfurt time, after the ECB publishes its
                      reference rates (around 16:00 CET). Incremental load.

edgar_quarterly       the 5th of Jan/Apr/Jul/Oct, when the SEC has published the
                      previous quarter's archive. ingest -> stage -> amendments ->
                      warehouse build + quality gate + publish.

Retries
    Every task retries with exponential backoff. Network steps get more
    attempts than compute steps; the quality gate gets none, because a failed
    check will fail again on the same data and retrying only delays the alert.

Timing ("SLA")
    Airflow 3 removed the old task-level SLA feature. Two mechanisms replace it:
    - hard limits: execution_timeout per task and dagrun_timeout per run, so a
      hung step becomes a failure (and an alert) instead of waiting forever;
    - soft target: a DeadlineAlert fires if a run has not finished within the
      target time after it was queued, even while it is still running.

Alerts go to ALERT_WEBHOOK_URL (Slack or Discord) via src/orchestration/alerts.py.

Heavy imports (Spark, DuckDB) happen inside the tasks, so the scheduler can
parse this file quickly.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from src.orchestration.alerts import on_deadline_missed, on_failure  # noqa: E402

DATA_DIR = Path(os.environ.get("EDGAR_DATA_DIR", REPO / "data"))


def _deadline(dag_id: str, interval: timedelta):
    """A DeadlineAlert on time since the run was queued; None on Airflow < 3.1."""
    try:
        from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
    except ImportError:
        return None
    return DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=interval,
        callback=AsyncCallback(on_deadline_missed,
                               kwargs={"dag_id": dag_id, "target": str(interval)}),
    )


BASE_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_failure,
}


@dag(
    dag_id="fx_rates_daily",
    schedule="0 17 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Berlin"),
    catchup=False,               # the loader's watermark already handles missed days
    max_active_runs=1,           # two loads must never upsert the same partition at once
    dagrun_timeout=timedelta(hours=1),
    deadline=_deadline("fx_rates_daily", timedelta(minutes=30)),
    default_args={**BASE_ARGS, "retries": 4, "retry_delay": timedelta(minutes=5)},
    tags=["fx", "incremental"],
)
def fx_rates_daily():
    @task(execution_timeout=timedelta(minutes=10))
    def load_fx_rates() -> dict:
        from src.pipeline import step_fx
        return step_fx(DATA_DIR)

    load_fx_rates()


@dag(
    dag_id="edgar_quarterly",
    schedule="0 6 5 1,4,7,10 *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    deadline=_deadline("edgar_quarterly", timedelta(hours=3)),
    default_args=BASE_ARGS,
    tags=["edgar"],
)
def edgar_quarterly():
    @task
    def previous_quarter(logical_date: datetime | None = None) -> dict:
        """The archive for the quarter that just ended."""
        from src.orchestration.schedule import previous_quarter as prev
        year, quarter = prev(logical_date or pendulum.now("UTC"))
        return {"year": year, "quarter": quarter}

    @task(retries=5, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=45))
    def ingest(q: dict) -> dict:
        from src.pipeline import step_ingest
        return step_ingest(DATA_DIR, q["year"], q["quarter"])

    @task(execution_timeout=timedelta(hours=2))
    def stage(q: dict) -> dict:
        from src.pipeline import step_stage
        return step_stage(DATA_DIR, q["year"], q["quarter"])

    @task(execution_timeout=timedelta(minutes=30))
    def amendments() -> dict:
        from src.pipeline import step_amendments
        return step_amendments(DATA_DIR)

    @task(retries=0, execution_timeout=timedelta(hours=1))
    def build_and_publish() -> str:
        from src.pipeline import step_warehouse
        return step_warehouse(DATA_DIR)

    q = previous_quarter()
    ingested = ingest(q)
    staged = stage(q)
    ingested >> staged
    staged >> amendments() >> build_and_publish()


fx_rates_daily()
edgar_quarterly()
