"""Quality gate for the warehouse build.

Every check is a SQL query that returns the number of offending rows. A check
passes when that number is zero (or, for rate checks, below its threshold).

    severity "error"  -> the build is not published
    severity "warn"   -> recorded and reported, the build is published

Results of every run, pass or fail, are written to the warehouse table
`quality_results` and appended to `data/warehouse/quality_log.jsonl`, so there
is a history of what was checked and when, including for builds that were
rejected.

Why plain SQL rather than a framework such as Great Expectations: the checks
run inside DuckDB against the full tables, with no data copied into Python,
and each one is a query that can be pasted into a SQL console to see the
offending rows. The trade-off is that there is no generated documentation
site; `quality_results` and the JSON log take its place.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

log = logging.getLogger("quality.checks")


@dataclass(frozen=True)
class Check:
    name: str
    severity: str          # "error" | "warn"
    description: str
    sql: str               # returns one integer: the number of failing rows
    table: str = ""        # dataset the check is about (for lineage)
    column: str | None = None
    max_failures: int = 0


@dataclass
class CheckResult:
    run_id: str
    checked_at: str
    name: str
    severity: str
    table: str
    column: str | None
    failures: int
    passed: bool
    description: str


class QualityGateError(RuntimeError):
    def __init__(self, failed: list[CheckResult], results: list[CheckResult]):
        self.failed = failed
        self.results = results
        super().__init__("quality gate failed: " + ", ".join(
            f"{r.name} ({r.failures})" for r in failed))


FACT_GRAIN = "filing_sk, tag_sk, date_sk, qtrs, uom, segments, coreg"


def checks(staged_num_rows: int | None = None, quarantine_rate: float | None = None,
           declared_lineage: dict[str, dict] | None = None) -> list[Check]:
    out = [
        # --- integrity of the fact table -------------------------------------
        Check("fact_grain_unique", "error",
              "One row per filing, tag, date, period length, unit and dimension",
              f"""SELECT coalesce(sum(n - 1), 0) FROM (
                      SELECT count(*) AS n FROM fact_financial_fact
                      GROUP BY {FACT_GRAIN} HAVING count(*) > 1)""",
              "fact_financial_fact"),
        Check("fact_filer_resolved", "error",
              "Every fact resolves to exactly the company version valid at filing time",
              "SELECT count(*) FROM fact_financial_fact WHERE filer_sk IS NULL",
              "fact_financial_fact", "filer_sk"),
        Check("fact_date_in_dim", "error", "Every fact date exists in dim_date",
              """SELECT count(*) FROM fact_financial_fact x
                 LEFT JOIN dim_date d USING (date_sk) WHERE d.date_sk IS NULL""",
              "fact_financial_fact", "date_sk"),
        Check("fact_value_not_null", "error", "No fact without a value",
              "SELECT count(*) FROM fact_financial_fact WHERE value IS NULL",
              "fact_financial_fact", "value"),
        Check("fact_tag_resolved", "error", "Every fact resolves to a tag",
              """SELECT count(*) FROM fact_financial_fact x
                 LEFT JOIN dim_tag t USING (tag_sk) WHERE t.tag_sk IS NULL""",
              "fact_financial_fact", "tag_sk"),
        Check("fact_tag_described", "warn",
              "Facts whose tag is not described in the tag table",
              """SELECT count(*) FROM fact_financial_fact x
                 JOIN dim_tag t USING (tag_sk) WHERE NOT t.in_tag_table""",
              "fact_financial_fact", "tag_sk"),

        # --- SCD2 on dim_filer ------------------------------------------------
        Check("filer_one_current_version", "error",
              "Every company has exactly one current version",
              """SELECT count(*) FROM (SELECT cik FROM dim_filer
                      GROUP BY cik HAVING sum(is_current::INT) <> 1)""",
              "dim_filer", "is_current"),
        Check("filer_intervals_valid", "error",
              "No version ends before it starts",
              "SELECT count(*) FROM dim_filer WHERE valid_to < valid_from",
              "dim_filer", "valid_to"),
        Check("filer_intervals_contiguous", "error",
              "Each version ends exactly when the next one starts: no gaps, no overlaps",
              """SELECT count(*) FROM (
                     SELECT valid_to, lead(valid_from) OVER (PARTITION BY cik ORDER BY valid_from, filer_sk) AS next_from
                     FROM dim_filer)
                 WHERE valid_to IS DISTINCT FROM next_from""",
              "dim_filer", "valid_from"),

        # --- filing versions ----------------------------------------------------
        Check("filing_one_latest_per_family", "error",
              "Every filing family has exactly one latest version",
              """SELECT count(*) FROM (SELECT family_id FROM dim_filing
                      GROUP BY family_id HAVING sum(is_latest::INT) <> 1)""",
              "dim_filing", "is_latest"),
        Check("filing_amendment_not_loaded", "warn",
              "SEC marks the filing as amended, but the amendment is not in the loaded quarters",
              "SELECT count(*) FROM dim_filing WHERE amendment_not_loaded",
              "dim_filing", "amendment_not_loaded"),

        # --- domain: the balance sheet must balance -------------------------------
        Check("balance_sheet_balances", "warn",
              "Assets = LiabilitiesAndStockholdersEquity for the same filing and date (tolerance 1 unit)",
              """SELECT count(*) FROM (
                     SELECT x.filing_sk, x.date_sk,
                            max(CASE WHEN t.tag = 'Assets' THEN x.value END) AS assets,
                            max(CASE WHEN t.tag = 'LiabilitiesAndStockholdersEquity' THEN x.value END) AS le
                     FROM fact_financial_fact x JOIN dim_tag t USING (tag_sk)
                     WHERE x.qtrs = 0 AND x.segments IS NULL AND x.coreg IS NULL
                       AND t.tag IN ('Assets', 'LiabilitiesAndStockholdersEquity')
                     GROUP BY x.filing_sk, x.date_sk)
                 WHERE assets IS NOT NULL AND le IS NOT NULL AND abs(assets - le) > 1""",
              "fact_financial_fact", "value"),
    ]

    if staged_num_rows is not None:
        out.append(Check(
            "fact_reconciles_to_staging", "error",
            "Fact rows equal staged fact rows: nothing lost or duplicated between layers",
            f"SELECT abs((SELECT count(*) FROM fact_financial_fact) - {int(staged_num_rows)})",
            "fact_financial_fact"))

    if quarantine_rate is not None:
        # Stored as basis points so the check can use the same integer contract.
        bps = round(quarantine_rate * 10_000)
        out.append(Check(
            "quarantine_rate_below_5pct", "error",
            "Share of raw fact rows rejected into quarantine stays below 5%",
            f"SELECT {bps}", "staging.num", max_failures=500))
        out.append(Check(
            "quarantine_rate_below_1pct", "warn",
            "Share of raw fact rows rejected into quarantine stays below 1%",
            f"SELECT {bps}", "staging.num", max_failures=100))

    if declared_lineage is not None:
        pairs = ", ".join(f"('{t}', '{c}')" for t, cols in declared_lineage.items() for c in cols)
        tables = ", ".join(f"'{t}'" for t in declared_lineage)
        out.append(Check(
            "lineage_declared_for_every_column", "error",
            "Every warehouse column has declared column-level lineage",
            f"""SELECT count(*) FROM information_schema.columns
                WHERE table_name IN ({tables})
                  AND (table_name, column_name) NOT IN (SELECT * FROM (VALUES {pairs}))""",
            "warehouse"))
    return out


def run(con: duckdb.DuckDBPyConnection, check_list: list[Check]) -> list[CheckResult]:
    run_id = str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    results = []
    for c in check_list:
        failures = int(con.execute(c.sql).fetchone()[0] or 0)
        results.append(CheckResult(run_id, checked_at, c.name, c.severity, c.table, c.column,
                                   failures, failures <= c.max_failures, c.description))
    return results


def record(con: duckdb.DuckDBPyConnection, results: list[CheckResult], log_file: Path) -> None:
    con.execute("""CREATE TABLE IF NOT EXISTS quality_results (
        run_id VARCHAR, checked_at VARCHAR, name VARCHAR, severity VARCHAR, "table" VARCHAR,
        "column" VARCHAR, failures BIGINT, passed BOOLEAN, description VARCHAR)""")
    con.executemany("INSERT INTO quality_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [tuple(asdict(r).values()) for r in results])
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as fh:
        for r in results:
            fh.write(json.dumps(asdict(r)) + "\n")


def gate(results: list[CheckResult]) -> None:
    for r in results:
        level = logging.INFO if r.passed else (logging.ERROR if r.severity == "error" else logging.WARNING)
        log.log(level, "%-36s %-5s failures=%d", r.name, r.severity, r.failures)
    failed = [r for r in results if r.severity == "error" and not r.passed]
    if failed:
        raise QualityGateError(failed, results)


def _count_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    if not path.exists() or not any(path.rglob("*.parquet")):
        return 0
    glob = str(path / "**" / "*.parquet").replace("\\", "/")
    return int(con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0])


def evaluate(warehouse: Path, data_dir: Path, declared_lineage: dict | None = None) -> list[CheckResult]:
    """Run every check against a built warehouse file, record the results, and
    raise QualityGateError if any error-level check failed."""
    con = duckdb.connect(str(warehouse))
    try:
        staged = _count_parquet(con, data_dir / "staging" / "num")
        quarantined = _count_parquet(con, data_dir / "quarantine" / "num")
        rate = quarantined / (staged + quarantined) if staged + quarantined else 0.0
        results = run(con, checks(staged, rate, declared_lineage))
        record(con, results, data_dir / "warehouse" / "quality_log.jsonl")
    finally:
        con.close()
    gate(results)
    return results
