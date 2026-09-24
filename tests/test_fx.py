from datetime import date
from decimal import Decimal

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.ingest.fx import fetch_ecb, load_incremental, parse_ecb_csv, read_watermark

ECB_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,OBS_STATUS
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956,A
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-03,1.0919,A
EXR.D.JPY.EUR.SP00.A,D,JPY,EUR,SP00,A,2024-01-02,155.52,A
EXR.D.JPY.EUR.SP00.A,D,JPY,EUR,SP00,A,2024-01-03,,H
"""


class FakeSource:
    """Serves a fixed table of rates, filtered to the requested window, and records calls."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def __call__(self, start, end, currencies):
        self.calls.append((start, end))
        df = pd.DataFrame(self.rows, columns=["date", "currency", "eur_rate"])
        if df.empty:
            return df
        df["eur_rate"] = df["eur_rate"].map(Decimal)
        return df[(df["date"] >= start) & (df["date"] <= end) & df["currency"].isin(currencies)].reset_index(drop=True)


RATES = [
    (date(2024, 1, 2), "USD", "1.0956"), (date(2024, 1, 2), "JPY", "155.52"),
    (date(2024, 1, 3), "USD", "1.0919"), (date(2024, 1, 3), "JPY", "155.00"),
    (date(2024, 1, 4), "USD", "1.0953"), (date(2024, 1, 4), "JPY", "156.10"),
]
CUR = ("USD", "JPY")


def stored(data_dir):
    files = sorted((data_dir / "staging" / "fx_rates").rglob("*.parquet"))
    return pd.concat([pq.read_table(f).to_pandas() for f in files]).sort_values(["date", "currency"])


def load(data_dir, source, today, **kw):
    return load_incremental(data_dir, today=today, since=date(2024, 1, 1), lookback_days=2,
                            currencies=CUR, fetch=source, **kw)


# --- parsing --------------------------------------------------------------------

def test_parse_ecb_csv_keeps_exact_decimals_and_skips_missing_fixings():
    df = parse_ecb_csv(ECB_CSV)
    assert len(df) == 3                                    # the empty JPY value is dropped
    assert df.loc[0, "eur_rate"] == Decimal("1.0956")
    assert df.loc[0, "date"] == date(2024, 1, 2)


def test_parse_rejects_unexpected_format():
    with pytest.raises(ValueError, match="missing columns"):
        parse_ecb_csv("a,b\n1,2\n")


def test_request_asks_only_for_the_window():
    class Session:
        def get(self, url, params, timeout):
            self.url, self.params = url, params
            return type("R", (), {"status_code": 200, "text": ECB_CSV, "raise_for_status": lambda s: None})()

    s = Session()
    fetch_ecb(date(2024, 1, 2), date(2024, 1, 3), ("USD", "JPY"), session=s)
    assert s.url.endswith("/EXR/D.USD+JPY.EUR.SP00.A")
    assert s.params == {"startPeriod": "2024-01-02", "endPeriod": "2024-01-03", "format": "csvdata"}


# --- incremental behaviour ------------------------------------------------------------

def test_first_run_backfills_from_since(tmp_path):
    src = FakeSource(RATES[:4])
    res = load(tmp_path, src, date(2024, 1, 3))
    assert src.calls == [(date(2024, 1, 1), date(2024, 1, 3))]
    assert (res.inserted, res.updated) == (4, 0)
    assert read_watermark(tmp_path) == date(2024, 1, 3)


def test_next_run_reads_only_from_watermark_minus_lookback(tmp_path):
    load(tmp_path, FakeSource(RATES[:4]), date(2024, 1, 3))
    src = FakeSource(RATES)
    res = load(tmp_path, src, date(2024, 1, 4))
    assert src.calls == [(date(2024, 1, 1), date(2024, 1, 4))]   # watermark 3 Jan - 2 days
    assert (res.inserted, res.updated, res.unchanged) == (2, 0, 4)
    assert len(stored(tmp_path)) == 6


def test_corrected_rate_inside_lookback_is_updated(tmp_path):
    load(tmp_path, FakeSource(RATES), date(2024, 1, 4))
    corrected = [r if r[:2] != (date(2024, 1, 3), "USD") else (r[0], r[1], "1.0920") for r in RATES]
    res = load(tmp_path, FakeSource(corrected), date(2024, 1, 5))
    assert res.updated == 1
    usd = stored(tmp_path).set_index(["date", "currency"]).loc[(date(2024, 1, 3), "USD"), "eur_rate"]
    assert usd == Decimal("1.0920")


def test_rerun_is_idempotent(tmp_path):
    load(tmp_path, FakeSource(RATES), date(2024, 1, 4))
    before = stored(tmp_path)
    res = load(tmp_path, FakeSource(RATES), date(2024, 1, 4))
    assert (res.inserted, res.updated) == (0, 0)
    pd.testing.assert_frame_equal(stored(tmp_path).reset_index(drop=True), before.reset_index(drop=True))


def test_day_without_publication_keeps_watermark(tmp_path):
    load(tmp_path, FakeSource(RATES), date(2024, 1, 4))
    res = load(tmp_path, FakeSource([]), date(2024, 1, 6))    # a Saturday: nothing published
    assert res.fetched == 0
    assert read_watermark(tmp_path) == date(2024, 1, 4)


def test_bad_batch_changes_nothing(tmp_path):
    load(tmp_path, FakeSource(RATES[:4]), date(2024, 1, 3))
    before = stored(tmp_path)
    bad = [r if r != RATES[5] else (r[0], r[1], "-1") for r in RATES]  # one negative rate
    with pytest.raises(ValueError, match="non-positive rate"):
        load(tmp_path, FakeSource(bad), date(2024, 1, 4))
    assert read_watermark(tmp_path) == date(2024, 1, 3)
    pd.testing.assert_frame_equal(stored(tmp_path).reset_index(drop=True), before.reset_index(drop=True))


def test_rows_are_partitioned_by_year(tmp_path):
    rows = [(date(2023, 12, 29), "USD", "1.1050"), (date(2024, 1, 2), "USD", "1.0956")]
    load_incremental(tmp_path, today=date(2024, 1, 2), since=date(2023, 12, 1),
                     currencies=("USD",), fetch=FakeSource(rows))
    parts = sorted(p.parent.name for p in (tmp_path / "staging" / "fx_rates").rglob("*.parquet"))
    assert parts == ["year=2023", "year=2024"]
