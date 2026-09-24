"""Incremental load of daily ECB euro reference rates.

Why FX rates here: some SEC filers (mostly foreign private issuers) report in
EUR, JPY, GBP and other currencies. The warehouse uses these rates to express
facts in USD.

Why this source is loaded differently from EDGAR: EDGAR arrives as whole
quarterly archives, so each quarter is simply replaced. Rates arrive one day at
a time, forever. Reloading all history every day would be wasteful, so this
loader keeps a watermark (the latest date loaded) and only asks the source for
what came after it.

Incremental rules
- Window: from (watermark - LOOKBACK_DAYS) to today. The overlap re-reads the
  last few days on every run, so a late publication or a corrected rate is
  picked up rather than missed.
- Upsert on (date, currency): new rows are inserted, changed rows updated,
  identical rows left alone. Running twice gives the same result as running once.
- Only the yearly partitions that received rows are rewritten, each through a
  temporary file and an atomic rename.
- The watermark moves forward only after every write succeeded, and never
  backwards. A failed run leaves both the data and the watermark as they were,
  so the next run simply retries the same window.
- A batch that fails validation (non-positive rate, duplicate key, date outside
  the requested window, unrequested currency) is rejected as a whole.

Output: data/staging/fx_rates/year=YYYY/part-0.parquet
State:  data/state/fx_watermark.json
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

log = logging.getLogger("ingest.fx")

ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.{currencies}.EUR.SP00.A"
CURRENCIES = ("USD", "JPY", "GBP", "CHF", "CNY", "CAD", "AUD", "HKD", "SEK", "NOK",
              "DKK", "KRW", "INR", "BRL", "MXN", "SGD", "ZAR", "ILS", "NZD", "PLN")
DEFAULT_SINCE = date(2019, 1, 1)
LOOKBACK_DAYS = 7

SCHEMA = pa.schema([
    ("date", pa.date32()),
    ("currency", pa.string()),
    ("eur_rate", pa.decimal128(18, 6)),   # units of `currency` per 1 EUR
    ("_loaded_at", pa.timestamp("us", tz="UTC")),
])

Fetcher = Callable[[date, date, tuple[str, ...]], pd.DataFrame]


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

def parse_ecb_csv(text: str) -> pd.DataFrame:
    """ECB SDMX CSV -> DataFrame(date, currency, eur_rate as Decimal)."""
    if not text.strip():
        return pd.DataFrame(columns=["date", "currency", "eur_rate"])
    raw = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
    missing = {"TIME_PERIOD", "CURRENCY", "OBS_VALUE"} - set(raw.columns)
    if missing:
        raise ValueError(f"unexpected ECB response, missing columns {sorted(missing)}")
    raw = raw[raw["OBS_VALUE"].str.strip() != ""]   # the ECB marks days without a fixing as empty
    return pd.DataFrame({
        "date": pd.to_datetime(raw["TIME_PERIOD"], format="%Y-%m-%d").dt.date,
        "currency": raw["CURRENCY"].str.strip(),
        "eur_rate": [Decimal(v.strip()) for v in raw["OBS_VALUE"]],
    }).reset_index(drop=True)


def fetch_ecb(start: date, end: date, currencies: tuple[str, ...] = CURRENCIES,
              session: requests.Session | None = None, retries: int = 3) -> pd.DataFrame:
    session = session or requests.Session()
    url = ECB_URL.format(currencies="+".join(currencies))
    params = {"startPeriod": start.isoformat(), "endPeriod": end.isoformat(), "format": "csvdata"}
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=60)
            if r.status_code == 404:          # the ECB answers 404 when the window has no data
                return parse_ecb_csv("")
            r.raise_for_status()
            return parse_ecb_csv(r.text)
        except requests.RequestException as exc:
            if attempt == retries:
                raise
            log.warning("ECB request failed (%s), retrying", exc)
    raise RuntimeError("unreachable")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _state_file(data_dir: Path) -> Path:
    return data_dir / "state" / "fx_watermark.json"


def read_watermark(data_dir: Path) -> date | None:
    path = _state_file(data_dir)
    if not path.exists():
        return None
    return date.fromisoformat(json.loads(path.read_text())["watermark"])


def _write_watermark(data_dir: Path, value: date) -> None:
    path = _state_file(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"watermark": value.isoformat(),
                               "updated_at": datetime.now(timezone.utc).isoformat()}))
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    window_start: str | None
    window_end: str | None
    fetched: int
    inserted: int
    updated: int
    unchanged: int
    watermark_before: str | None
    watermark_after: str | None


def _validate(df: pd.DataFrame, start: date, end: date, currencies: tuple[str, ...]) -> None:
    problems = []
    if (df["eur_rate"] <= 0).any():
        problems.append("non-positive rate")
    if df.duplicated(["date", "currency"]).any():
        problems.append("duplicate (date, currency)")
    if ((df["date"] < start) | (df["date"] > end)).any():
        problems.append("date outside requested window")
    if not set(df["currency"]).issubset(currencies):
        problems.append(f"unrequested currency {sorted(set(df['currency']) - set(currencies))}")
    if problems:
        raise ValueError("FX batch rejected: " + "; ".join(problems))


def _partition(data_dir: Path, year: int) -> Path:
    return data_dir / "staging" / "fx_rates" / f"year={year}" / "part-0.parquet"


def _read_partition(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in SCHEMA.names})
    return pq.read_table(path).to_pandas()


def _upsert_year(path: Path, new: pd.DataFrame, now: datetime) -> tuple[int, int, int]:
    old = _read_partition(path)
    merged = old.merge(new, on=["date", "currency"], how="outer",
                       suffixes=("_old", ""), indicator=True)
    is_new = merged["_merge"] == "right_only"
    both = merged["_merge"] == "both"
    changed = both & (merged["eur_rate"] != merged["eur_rate_old"])
    untouched = merged["_merge"] == "left_only"

    out = pd.DataFrame({
        "date": merged["date"],
        "currency": merged["currency"],
        "eur_rate": merged["eur_rate"].where(~untouched, merged["eur_rate_old"]),
        "_loaded_at": merged["_loaded_at"].where(both & ~changed | untouched, now)
                      if "_loaded_at" in merged else now,
    }).sort_values(["date", "currency"]).reset_index(drop=True)
    out["_loaded_at"] = pd.to_datetime(out["_loaded_at"], utc=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pandas(out, schema=SCHEMA, preserve_index=False), tmp)
    tmp.replace(path)
    return int(is_new.sum()), int(changed.sum()), int((both & ~changed).sum())


def load_incremental(data_dir: Path, today: date | None = None, since: date = DEFAULT_SINCE,
                     lookback_days: int = LOOKBACK_DAYS, currencies: tuple[str, ...] = CURRENCIES,
                     fetch: Fetcher | None = None) -> LoadResult:
    today = today or datetime.now(timezone.utc).date()
    fetch = fetch or (lambda s, e, c: fetch_ecb(s, e, c))
    before = read_watermark(data_dir)
    start = since if before is None else max(since, before - timedelta(days=lookback_days))
    if start > today:
        return LoadResult(None, None, 0, 0, 0, 0, _iso(before), _iso(before))

    batch = fetch(start, today, currencies)
    if batch.empty:
        log.info("no rates published between %s and %s", start, today)
        return LoadResult(start.isoformat(), today.isoformat(), 0, 0, 0, 0, _iso(before), _iso(before))
    _validate(batch, start, today, currencies)

    now = datetime.now(timezone.utc)
    inserted = updated = unchanged = 0
    for year, rows in batch.groupby(pd.to_datetime(batch["date"]).dt.year):
        i, u, n = _upsert_year(_partition(data_dir, int(year)), rows, now)
        inserted, updated, unchanged = inserted + i, updated + u, unchanged + n

    after = max(d for d in (before, max(batch["date"])) if d is not None)
    _write_watermark(data_dir, after)   # only after every partition is written
    result = LoadResult(start.isoformat(), today.isoformat(), len(batch), inserted, updated,
                        unchanged, _iso(before), after.isoformat())
    log.info("fx load: %s", asdict(result))
    return result


def _iso(d: date | None) -> str | None:
    return d.isoformat() if d else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally load ECB daily FX rates.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--since", type=date.fromisoformat, default=DEFAULT_SINCE,
                        help="first date to load when there is no watermark yet")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_incremental(args.data_dir, since=args.since)


if __name__ == "__main__":
    main()
