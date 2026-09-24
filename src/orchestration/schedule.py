"""Which EDGAR quarter a scheduled run should load."""

from __future__ import annotations

from datetime import date, datetime


def previous_quarter(run_date: date | datetime) -> tuple[int, int]:
    """The last complete calendar quarter before `run_date`.

    A run on 5 January loads Q4 of the previous year; a run on 5 April loads Q1.
    """
    q = (run_date.month - 1) // 3 + 1
    return (run_date.year - 1, 4) if q == 1 else (run_date.year, q - 1)
