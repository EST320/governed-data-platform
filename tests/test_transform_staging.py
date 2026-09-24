from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from src.transform.edgar_staging import NUM, SUB, TAG, stage_quarter, stage_table

LINEAGE = {"_source_file": "2024q1.zip/x.txt", "_ingested_at": "2024-04-01T00:00:00+00:00"}

SUB_ROWS = [
    # adsh,               cik,       name,          sic,    form,     period,     fy,     fp,   filed,      accepted,                prevrpt
    ("0000320193-24-000006", "320193", " APPLE INC ", "3571", "10-Q", "20231231", "2024", "Q1", "20240202", "2024-02-02 16:05:12.0", "0"),
    ("0000789019-24-000012", "789019", "MICROSOFT CORP", "7372", "10-Q/A", "20231231", "2024", "Q2", "20240131", "2024-01-30 16:10:00.0", "0"),
]
SUB_COLS = ["adsh", "cik", "name", "sic", "form", "period", "fy", "fp", "filed", "accepted", "prevrpt"]

NUM_COLS = ["adsh", "tag", "version", "ddate", "qtrs", "uom", "coreg", "value", "footnote"]
NUM_ROWS = [
    ("0000320193-24-000006", "Revenues", "us-gaap/2023", "20231231", "1", "USD", "", "119575000000", ""),
    ("0000320193-24-000006", "NetIncomeLoss", "us-gaap/2023", "20231231", "1", "USD", "", "33916000000.1234", ""),
    ("0000789019-24-000012", "Revenues", "us-gaap/2023", "20231231", "1", "USD", "", "62020000000", ""),
]

TAG_COLS = ["tag", "version", "custom", "abstract", "datatype", "tlabel"]
TAG_ROWS = [
    ("Revenues", "us-gaap/2023", "0", "0", "monetary", "Revenues"),
    ("NetIncomeLoss", "us-gaap/2023", "0", "0", "monetary", "Net Income (Loss)"),
]


def write_raw(data_dir: Path, table: str, cols, rows, year=2024, quarter=1):
    df = pd.DataFrame(rows, columns=cols).astype(str)
    for k, v in LINEAGE.items():
        df[k] = v
    part = data_dir / "raw" / table / f"year={year}" / f"quarter={quarter}"
    part.mkdir(parents=True, exist_ok=True)
    df.to_parquet(part / "part-0.parquet", index=False)


def read(data_dir: Path, layer: str, table: str) -> pd.DataFrame:
    part = data_dir / layer / table / "year=2024" / "quarter=1"
    files = list(part.glob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files]) if files else pd.DataFrame()


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    write_raw(tmp_path, "sub", SUB_COLS, SUB_ROWS)
    write_raw(tmp_path, "num", NUM_COLS, NUM_ROWS)
    write_raw(tmp_path, "tag", TAG_COLS, TAG_ROWS)
    return tmp_path


# --- typing -------------------------------------------------------------------

def test_types_are_parsed(spark, data_dir):
    stage_quarter(spark, data_dir, 2024, 1)
    sub = read(data_dir, "staging", "sub").set_index("adsh")
    apple = sub.loc["0000320193-24-000006"]
    assert apple["cik"] == 320193
    assert str(apple["period"]) == "2023-12-31"
    assert str(apple["accepted"]) == "2024-02-02 16:05:12"
    assert apple["prevrpt"] is False or apple["prevrpt"] == False  # noqa: E712
    assert apple["name"] == "APPLE INC"  # trimmed


def test_values_are_exact_decimals_not_floats(spark, data_dir):
    stage_quarter(spark, data_dir, 2024, 1)
    num = read(data_dir, "staging", "num")
    ni = num.loc[num["tag"] == "NetIncomeLoss", "value"].iloc[0]
    assert isinstance(ni, Decimal)
    assert ni == Decimal("33916000000.1234")


def test_amendment_flag_and_base_form(spark, data_dir):
    stage_quarter(spark, data_dir, 2024, 1)
    sub = read(data_dir, "staging", "sub").set_index("adsh")
    msft = sub.loc["0000789019-24-000012"]
    assert bool(msft["is_amendment"]) is True
    assert msft["base_form"] == "10-Q"
    assert bool(sub.loc["0000320193-24-000006", "is_amendment"]) is False


def test_optional_column_absent_in_older_quarters_becomes_null(spark, data_dir):
    # NUM_COLS has no `segments` column, as in pre-2024 archives
    stage_quarter(spark, data_dir, 2024, 1)
    assert read(data_dir, "staging", "num")["segments"].isna().all()


def test_empty_strings_become_null(spark, data_dir):
    stage_quarter(spark, data_dir, 2024, 1)
    num = read(data_dir, "staging", "num")
    assert num["footnote"].isna().all()
    assert num["coreg"].isna().all()


def test_lineage_columns_are_carried(spark, data_dir):
    stage_quarter(spark, data_dir, 2024, 1)
    num = read(data_dir, "staging", "num")
    assert set(num["_source_file"]) == {"2024q1.zip/x.txt"}
    assert num["_ingested_at"].notna().all() and num["_staged_at"].notna().all()


# --- quarantine -----------------------------------------------------------------

def test_unparseable_value_is_quarantined_not_nulled(spark, data_dir):
    rows = NUM_ROWS + [("0000320193-24-000006", "Assets", "us-gaap/2023", "20231231", "0", "USD", "", "12,345", "")]
    write_raw(data_dir, "num", NUM_COLS, rows)
    res = stage_quarter(spark, data_dir, 2024, 1)["num"]
    q = read(data_dir, "quarantine", "num")
    assert res.reasons == {"unparseable_value": 1}
    assert q["value"].iloc[0] == "12,345"          # raw value kept for inspection
    assert "Assets" not in set(read(data_dir, "staging", "num")["tag"])


def test_missing_required_field_is_quarantined(spark, data_dir):
    rows = SUB_ROWS + [("0000000001-24-000001", "1", "X CORP", "", "10-K", "20231231", "", "", "", "2024-03-01 09:00:00.0", "")]
    write_raw(data_dir, "sub", SUB_COLS, rows)
    res = stage_quarter(spark, data_dir, 2024, 1)["sub"]
    assert res.reasons == {"missing_filed": 1}


def test_bad_date_is_quarantined(spark, data_dir):
    rows = [NUM_ROWS[0], NUM_ROWS[2], ("0000320193-24-000006", "NetIncomeLoss", "us-gaap/2023", "20231399", "1", "USD", "", "1", "")]
    write_raw(data_dir, "num", NUM_COLS, rows)
    res = stage_quarter(spark, data_dir, 2024, 1)["num"]
    assert res.reasons == {"unparseable_ddate": 1}


def test_orphan_fact_is_quarantined(spark, data_dir):
    rows = NUM_ROWS + [("9999999999-24-999999", "Revenues", "us-gaap/2023", "20231231", "1", "USD", "", "5", "")]
    write_raw(data_dir, "num", NUM_COLS, rows)
    res = stage_quarter(spark, data_dir, 2024, 1)["num"]
    assert res.reasons == {"orphan_fact": 1}


def test_malformed_adsh_is_quarantined(spark, data_dir):
    rows = SUB_ROWS + [("not-an-adsh", "1", "X", "", "10-K", "20231231", "", "", "20240301", "2024-03-01 09:00:00.0", "")]
    write_raw(data_dir, "sub", SUB_COLS, rows)
    assert stage_quarter(spark, data_dir, 2024, 1)["sub"].reasons == {"malformed_adsh": 1}


# --- duplicates -----------------------------------------------------------------

def test_exact_duplicates_are_collapsed(spark, data_dir):
    write_raw(data_dir, "num", NUM_COLS, NUM_ROWS + [NUM_ROWS[0]])
    res = stage_quarter(spark, data_dir, 2024, 1)["num"]
    assert res.duplicates_dropped == 1 and res.staged == 3 and res.quarantined == 0


def test_conflicting_duplicates_are_all_quarantined(spark, data_dir):
    conflict = NUM_ROWS[0][:7] + ("120000000000", "")  # same key, different value
    write_raw(data_dir, "num", NUM_COLS, NUM_ROWS + [conflict])
    res = stage_quarter(spark, data_dir, 2024, 1)["num"]
    assert res.reasons == {"conflicting_duplicate": 2}
    assert res.staged == 2


# --- run-level guarantees -------------------------------------------------------

def test_row_conservation(spark, data_dir):
    rows = NUM_ROWS + [NUM_ROWS[0], ("0000320193-24-000006", "Assets", "us-gaap/2023", "20231231", "0", "USD", "", "abc", "")]
    write_raw(data_dir, "num", NUM_COLS, rows)
    for res in stage_quarter(spark, data_dir, 2024, 1).values():
        assert res.input_rows == res.staged + res.quarantined + res.duplicates_dropped


def test_rerun_is_idempotent_and_clears_old_quarantine(spark, data_dir):
    write_raw(data_dir, "num", NUM_COLS, NUM_ROWS + [("0000320193-24-000006", "Assets", "us-gaap/2023", "20231231", "0", "USD", "", "abc", "")])
    stage_quarter(spark, data_dir, 2024, 1)
    assert len(read(data_dir, "quarantine", "num")) == 1

    write_raw(data_dir, "num", NUM_COLS, NUM_ROWS)       # source fixed
    stage_quarter(spark, data_dir, 2024, 1)
    stage_quarter(spark, data_dir, 2024, 1)
    assert len(read(data_dir, "staging", "num")) == 3
    assert len(read(data_dir, "quarantine", "num")) == 0  # stale rejects do not survive


def test_missing_required_column_fails_loudly(spark, data_dir):
    cols = [c for c in NUM_COLS if c != "value"]
    write_raw(data_dir, "num", cols, [r[:7] + r[8:] for r in NUM_ROWS])
    with pytest.raises(ValueError, match="value"):
        stage_quarter(spark, data_dir, 2024, 1)


def test_missing_raw_partition_fails_loudly(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        stage_table(spark, TAG, tmp_path / "raw", tmp_path, 2024, 1)
