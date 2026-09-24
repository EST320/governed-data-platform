import json
import shutil

import duckdb
import pytest

from src.quality import checks as q
from src.warehouse.build import COLUMN_LINEAGE


@pytest.fixture
def db(warehouse_dir, tmp_path):
    """A writable copy of the published warehouse, to break on purpose."""
    data = tmp_path / "data"
    shutil.copytree(warehouse_dir, data)
    return data, data / "warehouse" / "edgar.duckdb"


def result(results, name):
    return next(r for r in results if r.name == name)


def run_on(path, staged=None, rate=None, lineage=None):
    con = duckdb.connect(str(path))
    try:
        return q.run(con, q.checks(staged, rate, lineage))
    finally:
        con.close()


def mutate(path, sql):
    con = duckdb.connect(str(path))
    con.execute(sql)
    con.close()


def test_clean_build_passes_every_error_check(db):
    data, path = db
    results = q.evaluate(path, data, COLUMN_LINEAGE)
    assert all(r.passed for r in results if r.severity == "error")


def test_warning_does_not_block(db):
    data, path = db
    results = q.evaluate(path, data, COLUMN_LINEAGE)   # fixture has one undescribed tag
    assert not result(results, "fact_tag_described").passed


def test_duplicate_fact_blocks(db):
    data, path = db
    mutate(path, "INSERT INTO fact_financial_fact SELECT * FROM fact_financial_fact LIMIT 1")
    with pytest.raises(q.QualityGateError) as err:
        q.evaluate(path, data, COLUMN_LINEAGE)
    assert {r.name for r in err.value.failed} == {"fact_grain_unique", "fact_reconciles_to_staging"}


def test_lost_fact_row_blocks(db):
    data, path = db
    mutate(path, "DELETE FROM fact_financial_fact WHERE qtrs = 1")
    with pytest.raises(q.QualityGateError) as err:
        q.evaluate(path, data, COLUMN_LINEAGE)
    assert result(err.value.failed, "fact_reconciles_to_staging").failures == 2


def test_overlapping_scd2_versions_block(db):
    _, path = db
    mutate(path, "UPDATE dim_filer SET valid_to = valid_to + INTERVAL 1 DAY WHERE cik = 1 AND NOT is_current")
    assert not result(run_on(path), "filer_intervals_contiguous").passed


def test_two_current_versions_block(db):
    _, path = db
    mutate(path, "UPDATE dim_filer SET is_current = TRUE WHERE cik = 1")
    assert result(run_on(path), "filer_one_current_version").failures == 1


def test_unbalanced_balance_sheet_warns(db):
    _, path = db
    mutate(path, """UPDATE fact_financial_fact SET value = value + 5
                    WHERE tag_sk = (SELECT tag_sk FROM dim_tag WHERE tag = 'Assets')""")
    r = result(run_on(path), "balance_sheet_balances")
    assert r.severity == "warn" and r.failures == 1


def test_undeclared_column_blocks(db):
    _, path = db
    mutate(path, "ALTER TABLE fact_financial_fact ADD COLUMN mystery INTEGER")
    assert result(run_on(path, lineage=COLUMN_LINEAGE), "lineage_declared_for_every_column").failures == 1


@pytest.mark.parametrize("rate,error_ok,warn_ok", [(0.004, True, True), (0.02, True, False), (0.06, False, False)])
def test_quarantine_rate_thresholds(db, rate, error_ok, warn_ok):
    _, path = db
    results = run_on(path, rate=rate)
    assert result(results, "quarantine_rate_below_5pct").passed is error_ok
    assert result(results, "quarantine_rate_below_1pct").passed is warn_ok


def test_results_are_recorded_even_when_the_gate_fails(db):
    data, path = db
    q.evaluate(path, data, COLUMN_LINEAGE)            # a passing run first
    mutate(path, "DELETE FROM fact_financial_fact")
    with pytest.raises(q.QualityGateError):
        q.evaluate(path, data, COLUMN_LINEAGE)
    con = duckdb.connect(str(path), read_only=True)
    runs = con.execute("SELECT count(DISTINCT run_id) FROM quality_results").fetchone()[0]
    con.close()
    assert runs == 2   # the passing run and the failing one
    lines = (data / "warehouse" / "quality_log.jsonl").read_text().splitlines()
    assert json.loads(lines[-1])["run_id"] != json.loads(lines[0])["run_id"]
