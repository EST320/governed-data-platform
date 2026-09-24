import zipfile
from pathlib import Path

import pandas as pd
import pytest

from src.ingest.edgar import land, quarter_name

SUB = "adsh\tcik\tname\tsic\n0000001-24-000001\t320193\tAPPLE INC\t3571\n0000002-24-000002\t789019\tMICROSOFT CORP\t7372\n"
NUM = 'adsh\ttag\tversion\tddate\tqtrs\tuom\tvalue\n0000001-24-000001\tRevenues\tus-gaap/2023\t20231231\t1\tUSD\t119575000000\n0000001-24-000001\tNetIncomeLoss\tus-gaap/2023\t20231231\t1\tUSD\t33916000000\n0000002-24-000002\tRevenues\tus-gaap/2023\t20231231\t1\tUSD\t62020000000\n'
TAG = 'tag\tversion\tdatatype\ttlabel\nRevenues\tus-gaap/2023\tmonetary\tRevenues "net"\n'


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    path = tmp_path / "2024q1.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("sub.txt", SUB)
        zf.writestr("num.txt", NUM)
        zf.writestr("tag.txt", TAG)
    return path


def read(raw: Path, table: str) -> pd.DataFrame:
    return pd.read_parquet(raw / table / "year=2024" / "quarter=1")


def test_quarter_name():
    assert quarter_name(2024, 1) == "2024q1"


@pytest.mark.parametrize("year,q", [(2024, 0), (2024, 5), (2008, 1)])
def test_quarter_name_rejects_invalid(year, q):
    with pytest.raises(ValueError):
        quarter_name(year, q)


def test_land_row_counts(archive, tmp_path):
    counts = land(archive, tmp_path / "raw", 2024, 1)
    assert counts == {"sub": 2, "num": 3, "tag": 1}


def test_land_keeps_everything_as_string(archive, tmp_path):
    raw = tmp_path / "raw"
    land(archive, raw, 2024, 1)
    num = read(raw, "num")
    assert num["value"].iloc[0] == "119575000000"  # no float rounding in raw
    assert read(raw, "sub")["cik"].iloc[0] == "320193"


def test_land_preserves_stray_quotes(archive, tmp_path):
    raw = tmp_path / "raw"
    land(archive, raw, 2024, 1)
    assert read(raw, "tag")["tlabel"].iloc[0] == 'Revenues "net"'


def test_land_adds_lineage_columns(archive, tmp_path):
    raw = tmp_path / "raw"
    land(archive, raw, 2024, 1)
    num = read(raw, "num")
    assert set(num["_source_file"]) == {"2024q1.zip/num.txt"}
    assert num["_ingested_at"].notna().all()


def test_land_is_idempotent(archive, tmp_path):
    raw = tmp_path / "raw"
    land(archive, raw, 2024, 1)
    land(archive, raw, 2024, 1)
    assert len(read(raw, "num")) == 3
    assert len(list((raw / "num" / "year=2024" / "quarter=1").glob("*.parquet"))) == 1


def test_land_fails_loudly_on_missing_table(tmp_path):
    bad = tmp_path / "2024q1.zip"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("sub.txt", SUB)
    with pytest.raises(FileNotFoundError):
        land(bad, tmp_path / "raw", 2024, 1)
