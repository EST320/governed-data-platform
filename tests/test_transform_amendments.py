from pathlib import Path

import pandas as pd
import pytest

from src.transform.amendments import resolve_amendments
from src.transform.edgar_staging import SUB, stage_table

COLS = ["adsh", "cik", "name", "form", "period", "fy", "fp", "filed", "accepted", "prevrpt"]
LINEAGE = {"_source_file": "x.zip/sub.txt", "_ingested_at": "2024-07-01T00:00:00+00:00"}

# Acme files its FY2023 10-K in Q1, then two amendments in Q2.
Q1 = [
    ("0000000001-24-000001", "1", "ACME", "10-K", "20231231", "2023", "FY", "20240215", "2024-02-15 17:00:00.0", "1"),
    ("0000000002-24-000001", "2", "BETA", "10-K", "20231231", "2023", "FY", "20240220", "2024-02-20 17:00:00.0", "0"),
]
Q2 = [
    ("0000000001-24-000010", "1", "ACME", "10-K/A", "20231231", "2023", "FY", "20240420", "2024-04-20 09:00:00.0", "1"),
    ("0000000001-24-000020", "1", "ACME", "10-K/A", "20231231", "2023", "FY", "20240501", "2024-05-01 09:00:00.0", "0"),
]


def write_raw(data_dir: Path, rows, year, quarter):
    df = pd.DataFrame(rows, columns=COLS)
    for k, v in LINEAGE.items():
        df[k] = v
    part = data_dir / "raw" / "sub" / f"year={year}" / f"quarter={quarter}"
    part.mkdir(parents=True, exist_ok=True)
    df.to_parquet(part / "part-0.parquet", index=False)


def stage(spark, data_dir, quarters):
    for (y, q), rows in quarters.items():
        write_raw(data_dir, rows, y, q)
        stage_table(spark, SUB, data_dir / "raw", data_dir, y, q)


def versions(data_dir: Path) -> pd.DataFrame:
    return pd.read_parquet(data_dir / "staging" / "filing_versions").set_index("adsh")


@pytest.fixture
def data_dir(spark, tmp_path):
    stage(spark, tmp_path, {(2024, 1): Q1, (2024, 2): Q2})
    return tmp_path


def test_versions_are_ordered_by_acceptance_time(spark, data_dir):
    resolve_amendments(spark, data_dir)
    v = versions(data_dir)
    assert v.loc["0000000001-24-000001", "version_no"] == 1
    assert v.loc["0000000001-24-000010", "version_no"] == 2
    assert v.loc["0000000001-24-000020", "version_no"] == 3
    assert (v.loc[v["cik"] == 1, "n_versions"] == 3).all()


def test_only_the_last_version_is_latest(spark, data_dir):
    resolve_amendments(spark, data_dir)
    v = versions(data_dir)
    acme = v[v["cik"] == 1]
    assert list(acme.index[acme["is_latest"]]) == ["0000000001-24-000020"]


def test_versions_are_linked_both_ways(spark, data_dir):
    resolve_amendments(spark, data_dir)
    v = versions(data_dir)
    first_amendment = v.loc["0000000001-24-000010"]
    assert first_amendment["supersedes_adsh"] == "0000000001-24-000001"
    assert first_amendment["superseded_by_adsh"] == "0000000001-24-000020"


def test_validity_intervals_do_not_overlap(spark, data_dir):
    resolve_amendments(spark, data_dir)
    v = versions(data_dir)
    original = v.loc["0000000001-24-000001"]
    assert str(original["valid_from"]) == "2024-02-15 17:00:00"
    assert str(original["valid_to"]) == "2024-04-20 09:00:00"   # ends when the first amendment arrives
    assert pd.isna(v.loc["0000000001-24-000020", "valid_to"])    # current version is open-ended


def test_families_are_separate_per_company(spark, data_dir):
    resolve_amendments(spark, data_dir)
    beta = versions(data_dir).loc["0000000002-24-000001"]
    assert beta["version_no"] == 1 and beta["n_versions"] == 1 and bool(beta["is_latest"])


def test_amendment_whose_original_is_outside_loaded_range(spark, tmp_path):
    stage(spark, tmp_path, {(2024, 2): Q2})  # the original 10-K was never loaded
    stats = resolve_amendments(spark, tmp_path)
    v = versions(tmp_path)
    assert bool(v.loc["0000000001-24-000010", "original_not_loaded"])
    assert stats["original_not_loaded"] == 1


def test_sec_says_amended_but_amendment_not_loaded(spark, tmp_path):
    stage(spark, tmp_path, {(2024, 1): Q1})  # Q2 with the amendments not loaded yet
    stats = resolve_amendments(spark, tmp_path)
    assert bool(versions(tmp_path).loc["0000000001-24-000001", "amendment_not_loaded"])
    assert stats["amendment_not_loaded"] == 1


def test_two_originals_for_one_period_are_flagged(spark, tmp_path):
    dup = ("0000000002-24-000099", "2", "BETA", "10-K", "20231231", "2023", "FY", "20240301", "2024-03-01 08:00:00.0", "0")
    stage(spark, tmp_path, {(2024, 1): Q1 + [dup]})
    resolve_amendments(spark, tmp_path)
    beta = versions(tmp_path)
    assert beta.loc[beta["cik"] == 2, "duplicate_original"].all()


def test_rebuild_is_idempotent(spark, data_dir):
    first = resolve_amendments(spark, data_dir)
    second = resolve_amendments(spark, data_dir)
    assert first == second
    assert len(versions(data_dir)) == 4
    assert not (data_dir / "staging" / "filing_versions._tmp").exists()


def test_fails_loudly_without_staged_filings(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        resolve_amendments(spark, tmp_path)
