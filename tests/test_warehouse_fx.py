import shutil
from datetime import date
from decimal import Decimal

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingest.fx import load_incremental
from src.quality import checks as q
from src.warehouse.build import COLUMN_LINEAGE, build_candidate
from tests.test_fx import FakeSource


@pytest.fixture
def fx_warehouse(warehouse_dir, tmp_path):
    """The fixture warehouse plus one EUR-denominated fact and a few days of rates."""
    data = tmp_path / "data"
    shutil.copytree(warehouse_dir, data)

    part = data / "staging" / "num" / "year=2024" / "quarter=1"
    template = pq.read_table(next(part.glob("*.parquet")))
    row = template.slice(0, 1).to_pandas()
    row[["tag", "uom", "value", "qtrs"]] = ["Revenues", "EUR", Decimal("1000.0000"), 4]
    row["ddate"] = date(2023, 12, 31)                          # a Sunday: no fixing that day
    pq.write_table(pa.Table.from_pandas(row, schema=template.schema, preserve_index=False),
                   part / "eur-fact.parquet")

    rates = [(date(2023, 12, 28), "USD", "1.1000"), (date(2023, 12, 28), "JPY", "156.00"),
             (date(2023, 12, 29), "USD", "1.1050"), (date(2023, 12, 29), "JPY", "156.33")]
    load_incremental(data, today=date(2023, 12, 31), since=date(2023, 12, 1),
                     currencies=("USD", "JPY"), fetch=FakeSource(rates))
    candidate = build_candidate(data)
    return data, candidate


def test_eur_fact_converted_with_last_rate_before_the_date(fx_warehouse):
    _, db = fx_warehouse
    con = duckdb.connect(str(db), read_only=True)
    value_usd, fx_date = con.execute(
        "SELECT value_usd, fx_date FROM v_facts_current_usd WHERE uom = 'EUR'").fetchone()
    con.close()
    assert fx_date == date(2023, 12, 29)          # Friday's fixing used for Sunday's date
    assert value_usd == Decimal("1105.0000")      # 1000 EUR * 1.1050 USD/EUR


def test_usd_facts_pass_through_unchanged(fx_warehouse):
    _, db = fx_warehouse
    con = duckdb.connect(str(db), read_only=True)
    n_bad = con.execute("SELECT count(*) FROM v_facts_current_usd "
                        "WHERE uom = 'USD' AND (value_usd <> value OR fx_date IS NOT NULL)").fetchone()[0]
    con.close()
    assert n_bad == 0


def test_cross_rate_through_eur(fx_warehouse):
    _, db = fx_warehouse
    con = duckdb.connect(str(db), read_only=True)
    jpy = con.execute("SELECT usd_per_unit FROM fx_usd_daily "
                      "WHERE currency = 'JPY' AND date = DATE '2023-12-29'").fetchone()[0]
    con.close()
    assert float(jpy) == pytest.approx(1.1050 / 156.33, rel=1e-9)


def test_gate_passes_with_fx(fx_warehouse):
    data, db = fx_warehouse
    results = q.evaluate(db, data, COLUMN_LINEAGE)
    assert {r.name: r.passed for r in results}["fx_coverage"] is True


def test_warehouse_builds_without_fx(warehouse_dir):
    con = duckdb.connect(str(warehouse_dir / "warehouse" / "edgar.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM fx_rate_daily").fetchone()[0] == 0
    con.close()
