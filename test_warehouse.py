import shutil
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from src.warehouse.build import publish
from tests.pipeline_fixtures import ACME_10K, ACME_10KA, ACME_10Q, BETA_10K


@pytest.fixture
def con(warehouse_dir):
    c = duckdb.connect(str(warehouse_dir / "warehouse" / "edgar.duckdb"), read_only=True)
    yield c
    c.close()


def one(con, sql, *params):
    return con.execute(sql, list(params)).fetchone()


def revenue_as_of(con, cik, ddate, ts):
    row = one(con, "SELECT value, adsh FROM facts_as_of(?::TIMESTAMP) "
                   "WHERE cik = ? AND tag = 'Revenues' AND ddate = ?::DATE AND qtrs = 4", ts, cik, ddate)
    return row


# --- star schema ------------------------------------------------------------------

def test_every_staged_fact_becomes_one_fact_row(con):
    assert one(con, "SELECT count(*) FROM fact_financial_fact")[0] == 7


def test_values_stay_exact_decimals(con):
    value = one(con, "SELECT value FROM fact_financial_fact WHERE adsh = ? AND qtrs = 4", BETA_10K)[0]
    assert value == Decimal("200.0000")


def test_dim_filer_is_scd2_on_name_change(con):
    rows = con.execute("SELECT name, valid_from, valid_to, is_current FROM dim_filer "
                       "WHERE cik = 1 ORDER BY valid_from").fetchall()
    assert [r[0] for r in rows] == ["ACME CORP", "ACME HOLDINGS"]
    assert rows[0][2] == datetime(2024, 4, 20, 9, 0) == rows[1][1]   # contiguous
    assert [r[3] for r in rows] == [False, True]


def test_fact_carries_company_name_valid_at_filing_time(con):
    names = dict(con.execute("""SELECT x.adsh, f.name FROM fact_financial_fact x
                                JOIN dim_filer f USING (filer_sk) WHERE x.qtrs = 4""").fetchall())
    assert names[ACME_10K] == "ACME CORP"          # before the rename
    assert names[ACME_10KA] == "ACME HOLDINGS"     # after the rename


def test_undescribed_tags_get_their_own_row(con):
    row = one(con, "SELECT in_tag_table, datatype FROM dim_tag WHERE tag = 'CustomMetric'")
    assert row == (False, None)


def test_dim_date_covers_every_fact(con):
    assert one(con, """SELECT count(*) FROM fact_financial_fact x
                       LEFT JOIN dim_date d USING (date_sk) WHERE d.date IS NULL""")[0] == 0
    assert one(con, "SELECT is_quarter_end FROM dim_date WHERE date = DATE '2024-03-31'")[0] is True


# --- point in time ----------------------------------------------------------------------

def test_as_of_before_amendment_returns_original_value(con):
    assert revenue_as_of(con, 1, "2023-12-31", "2024-03-01") == (Decimal("100.0000"), ACME_10K)


def test_as_of_after_amendment_returns_restated_value(con):
    assert revenue_as_of(con, 1, "2023-12-31", "2024-04-21") == (Decimal("110.0000"), ACME_10KA)


def test_as_of_before_any_filing_returns_nothing(con):
    assert revenue_as_of(con, 1, "2023-12-31", "2024-01-01") is None


def test_as_of_is_exact_at_the_acceptance_second(con):
    assert revenue_as_of(con, 1, "2023-12-31", "2024-04-20 08:59:59")[0] == Decimal("100.0000")
    assert revenue_as_of(con, 1, "2023-12-31", "2024-04-20 09:00:00")[0] == Decimal("110.0000")


def test_current_view_has_one_value_per_fact(con):
    assert one(con, """SELECT count(*) FROM (SELECT cik, tag, ddate, qtrs, uom FROM v_facts_current
                       GROUP BY ALL HAVING count(*) > 1)""")[0] == 0
    assert one(con, "SELECT value FROM v_facts_current WHERE cik = 1 AND tag = 'Revenues' "
                    "AND ddate = DATE '2023-12-31'")[0] == Decimal("110.0000")


# --- publishing -----------------------------------------------------------------------

def test_failed_gate_leaves_live_warehouse_untouched(warehouse_dir, tmp_path):
    data = tmp_path / "data"
    shutil.copytree(warehouse_dir, data)
    live = data / "warehouse" / "edgar.duckdb"
    before = live.read_bytes()

    def failing_gate(_):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        publish(data, failing_gate)
    assert live.read_bytes() == before
    assert (data / "warehouse" / "edgar.failed.duckdb").exists()
    assert not (data / "warehouse" / "edgar.building.duckdb").exists()


def test_passing_gate_replaces_live_and_clears_old_failure(warehouse_dir, tmp_path):
    data = tmp_path / "data"
    shutil.copytree(warehouse_dir, data)
    (data / "warehouse" / "edgar.failed.duckdb").write_bytes(b"old")
    publish(data, lambda _: None)
    assert (data / "warehouse" / "edgar.duckdb").exists()
    assert not (data / "warehouse" / "edgar.failed.duckdb").exists()
