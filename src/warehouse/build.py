"""Staging -> DuckDB star schema.

Tables
    dim_date                calendar, one row per day between the earliest and latest date referenced
    dim_filer   (SCD2)      one row per company per attribute version (name, SIC, state, country)
    dim_tag                 one row per XBRL tag and taxonomy version used or described
    dim_filing              one row per filing, with its version chain from staging.filing_versions
    fact_financial_fact     one row per staged numeric fact

Point-in-time access
    facts_as_of(ts)         table macro: for every fact, the value from the most recently
                            accepted filing that was public at `ts`
    v_facts_current         the same, as of now

Build discipline
    The warehouse is built into a new file next to the live one. The quality gate
    runs against that file, and only a build that passes replaces the live
    warehouse. A failed build is kept as `*.failed.duckdb` for inspection; the
    live warehouse is never left half-built or holding data that failed checks.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb

log = logging.getLogger("warehouse.build")

WAREHOUSE_FILE = "edgar.duckdb"

# Declared column-level lineage: warehouse column -> staging columns it is derived from.
# Emitted to OpenLineage, and checked by the quality gate so that a new column
# cannot be added to the warehouse without saying where it comes from.
COLUMN_LINEAGE: dict[str, dict[str, list[tuple[str, str]]]] = {
    "dim_date": {
        "date_sk": [("staging.num", "ddate"), ("staging.sub", "period"), ("staging.sub", "filed")],
        "date": [("staging.num", "ddate"), ("staging.sub", "period"), ("staging.sub", "filed")],
        "year": [("staging.num", "ddate")],
        "quarter": [("staging.num", "ddate")],
        "month": [("staging.num", "ddate")],
        "day_of_week": [("staging.num", "ddate")],
        "is_quarter_end": [("staging.num", "ddate")],
    },
    "dim_filer": {
        "filer_sk": [("staging.sub", "cik"), ("staging.sub", "accepted")],
        "cik": [("staging.sub", "cik")],
        "name": [("staging.sub", "name")],
        "sic": [("staging.sub", "sic")],
        "countryba": [("staging.sub", "countryba")],
        "stprba": [("staging.sub", "stprba")],
        "valid_from": [("staging.sub", "accepted")],
        "valid_to": [("staging.sub", "accepted")],
        "is_current": [("staging.sub", "accepted")],
    },
    "dim_tag": {
        "tag_sk": [("staging.tag", "tag"), ("staging.tag", "version")],
        "tag": [("staging.tag", "tag")],
        "version": [("staging.tag", "version")],
        "custom": [("staging.tag", "custom")],
        "abstract": [("staging.tag", "abstract")],
        "datatype": [("staging.tag", "datatype")],
        "crdr": [("staging.tag", "crdr")],
        "iord": [("staging.tag", "iord")],
        "tlabel": [("staging.tag", "tlabel")],
        "in_tag_table": [("staging.tag", "tag"), ("staging.num", "tag")],
    },
    "dim_filing": {
        c: [("staging.filing_versions", c)] for c in (
            "filing_sk", "adsh", "family_id", "cik", "form", "base_form", "is_amendment",
            "fy", "fp", "period", "filed", "accepted", "version_no", "n_versions", "is_latest",
            "valid_from", "valid_to", "original_not_loaded", "amendment_not_loaded",
            "duplicate_original")
    },
    "fact_financial_fact": {
        "filing_sk": [("staging.filing_versions", "adsh")],
        "filer_sk": [("staging.sub", "cik"), ("staging.sub", "accepted")],
        "tag_sk": [("staging.num", "tag"), ("staging.num", "version")],
        "date_sk": [("staging.num", "ddate")],
        "adsh": [("staging.num", "adsh")],
        "qtrs": [("staging.num", "qtrs")],
        "uom": [("staging.num", "uom")],
        "segments": [("staging.num", "segments")],
        "coreg": [("staging.num", "coreg")],
        "value": [("staging.num", "value")],
        "known_at": [("staging.sub", "accepted")],
    },
    "fx_rate_daily": {
        "date": [("staging.fx_rates", "date")],
        "currency": [("staging.fx_rates", "currency")],
        "eur_rate": [("staging.fx_rates", "eur_rate")],
    },
    "fx_usd_daily": {
        "date": [("staging.fx_rates", "date")],
        "currency": [("staging.fx_rates", "currency")],
        "usd_per_unit": [("staging.fx_rates", "eur_rate")],
    },
}


def _parquet(path: Path) -> str:
    return str(path / "**" / "*.parquet").replace("\\", "/")


def _register_staging(con: duckdb.DuckDBPyConnection, data_dir: Path) -> None:
    staging = data_dir / "staging"
    for table in ("sub", "num", "tag"):
        if not (staging / table).exists():
            raise FileNotFoundError(f"staging table missing: {staging / table}")
        con.execute(f"CREATE OR REPLACE TEMP VIEW stg_{table} AS "
                    f"SELECT * FROM read_parquet('{_parquet(staging / table)}', hive_partitioning = true)")
    versions = staging / "filing_versions"
    if not versions.exists():
        raise FileNotFoundError(f"{versions} missing; run the amendments step first")
    con.execute(f"CREATE OR REPLACE TEMP VIEW stg_filing_versions AS "
                f"SELECT * FROM read_parquet('{_parquet(versions)}')")

    fx = staging / "fx_rates"
    if fx.exists() and any(fx.rglob("*.parquet")):
        con.execute(f"CREATE OR REPLACE TEMP VIEW stg_fx_rates AS "
                    f"SELECT date, currency, eur_rate FROM read_parquet('{_parquet(fx)}', hive_partitioning = true)")
    else:  # FX is optional: without it the USD view is simply limited to USD facts
        con.execute("CREATE OR REPLACE TEMP TABLE stg_fx_rates "
                    "(date DATE, currency VARCHAR, eur_rate DECIMAL(18,6))")


SCHEMA_SQL = r"""
-- Spark writes timestamps as UTC instants; keep them as plain UTC timestamps here.
CREATE TABLE dim_filing AS
SELECT
    row_number() OVER (ORDER BY accepted, adsh)::INTEGER AS filing_sk,
    adsh, family_id, cik, form, base_form, is_amendment, fy, fp, period, filed,
    accepted::TIMESTAMP AS accepted,
    version_no, n_versions, is_latest,
    valid_from::TIMESTAMP AS valid_from,
    valid_to::TIMESTAMP   AS valid_to,
    original_not_loaded, amendment_not_loaded, duplicate_original
FROM stg_filing_versions;

-- SCD2: a new version starts whenever a tracked attribute differs from the
-- company's previous filing, and is valid from that filing's acceptance time.
CREATE TABLE dim_filer AS
WITH filings AS (
    SELECT cik, name, sic, countryba, stprba, adsh, accepted::TIMESTAMP AS accepted,
           md5(concat_ws('|', coalesce(name, ''), coalesce(sic::VARCHAR, ''),
                              coalesce(countryba, ''), coalesce(stprba, ''))) AS attr_hash
    FROM stg_sub
),
flagged AS (
    SELECT *, lag(attr_hash) OVER (PARTITION BY cik ORDER BY accepted, adsh) AS prev_hash
    FROM filings
),
changes AS (
    SELECT * FROM flagged WHERE prev_hash IS NULL OR prev_hash <> attr_hash
)
SELECT
    row_number() OVER (ORDER BY cik, accepted, adsh)::INTEGER AS filer_sk,
    cik, name, sic, countryba, stprba,
    accepted AS valid_from,
    lead(accepted) OVER (PARTITION BY cik ORDER BY accepted, adsh) AS valid_to,
    lead(accepted) OVER (PARTITION BY cik ORDER BY accepted, adsh) IS NULL AS is_current
FROM changes;

-- Type 1 is enough here: the taxonomy version is part of the key, so a changed
-- definition arrives as a new key rather than as a change to an existing row.
-- Tags used by facts but missing from the tag table still get their own row
-- (flagged), so two different unknown tags are never merged into one.
CREATE TABLE dim_tag AS
WITH described AS (
    SELECT *, row_number() OVER (PARTITION BY tag, version ORDER BY year DESC, quarter DESC) AS rn
    FROM stg_tag
),
used AS (
    SELECT DISTINCT tag, version FROM stg_num
),
all_tags AS (
    SELECT tag, version, custom, abstract, datatype, crdr, iord, tlabel, TRUE AS in_tag_table
    FROM described WHERE rn = 1
    UNION ALL
    SELECT u.tag, u.version, NULL, NULL, NULL, NULL, NULL, NULL, FALSE
    FROM used u ANTI JOIN described d ON d.tag = u.tag AND d.version = u.version AND d.rn = 1
)
SELECT row_number() OVER (ORDER BY tag, version)::INTEGER AS tag_sk, *
FROM all_tags;

CREATE TABLE dim_date AS
WITH bounds AS (
    SELECT min(d) AS lo, max(d) AS hi FROM (
        SELECT ddate AS d FROM stg_num UNION ALL
        SELECT period FROM stg_sub UNION ALL
        SELECT filed FROM stg_sub)
)
SELECT
    (year(d) * 10000 + month(d) * 100 + day(d))::INTEGER AS date_sk,
    d::DATE AS date,
    year(d)::INTEGER AS year,
    quarter(d)::INTEGER AS quarter,
    month(d)::INTEGER AS month,
    dayofweek(d)::INTEGER AS day_of_week,
    (d::DATE = last_day(d::DATE) AND month(d) IN (3, 6, 9, 12)) AS is_quarter_end
FROM bounds, generate_series(lo::TIMESTAMP, hi::TIMESTAMP, INTERVAL 1 DAY) AS t(d);

-- One row per staged fact. The company is resolved as of the filing's
-- acceptance time, so a fact carries the name the company had when it reported.
CREATE TABLE fact_financial_fact AS
SELECT
    f.filing_sk,
    fl.filer_sk,
    t.tag_sk,
    (year(n.ddate) * 10000 + month(n.ddate) * 100 + day(n.ddate))::INTEGER AS date_sk,
    n.adsh, n.qtrs, n.uom, n.segments, n.coreg, n.value,
    f.accepted AS known_at
FROM stg_num n
JOIN dim_filing f ON f.adsh = n.adsh
LEFT JOIN dim_filer fl
       ON fl.cik = f.cik
      AND f.accepted >= fl.valid_from
      AND (fl.valid_to IS NULL OR f.accepted < fl.valid_to)
JOIN dim_tag t ON t.tag = n.tag AND t.version = n.version;

-- Point in time. The same number (company, concept, period) is often reported
-- more than once: in an amendment, or as a comparative figure in next year's
-- report. As of a timestamp, the applicable value is the one from the most
-- recently accepted filing that was already public.
CREATE MACRO facts_as_of(as_of_ts) AS TABLE
SELECT cik, tag, ddate, qtrs, uom, segments, coreg, value, adsh, form, known_at
FROM (
    SELECT fl.cik, t.tag, d.date AS ddate, x.qtrs, x.uom, x.segments, x.coreg, x.value,
           x.adsh, fl.form, x.known_at,
           row_number() OVER (
               PARTITION BY fl.cik, t.tag, x.date_sk, x.qtrs, x.uom, x.segments, x.coreg
               ORDER BY x.known_at DESC, x.adsh DESC) AS rn
    FROM fact_financial_fact x
    JOIN dim_filing fl USING (filing_sk)
    JOIN dim_tag t USING (tag_sk)
    JOIN dim_date d USING (date_sk)
    WHERE x.known_at <= as_of_ts
)
WHERE rn = 1;

CREATE VIEW v_facts_current AS
SELECT * FROM facts_as_of(TIMESTAMP '9999-12-31 00:00:00');

-- ECB reference rates: units of `currency` per 1 EUR.
CREATE TABLE fx_rate_daily AS
SELECT date::DATE AS date, currency, eur_rate::DECIMAL(18,6) AS eur_rate FROM stg_fx_rates;

-- USD per one unit of each currency, derived through EUR (the ECB's base).
CREATE TABLE fx_usd_daily AS
SELECT r.date, r.currency, (u.eur_rate::DOUBLE / r.eur_rate::DOUBLE)::DECIMAL(24,12) AS usd_per_unit
FROM fx_rate_daily r
JOIN fx_rate_daily u ON u.date = r.date AND u.currency = 'USD'
WHERE r.currency <> 'USD'
UNION ALL
SELECT date, 'EUR', eur_rate::DECIMAL(24,12) FROM fx_rate_daily WHERE currency = 'USD';

-- Monetary facts in USD. Non-USD values use the latest rate on or before the
-- fact date (ASOF join), because there is no fixing on weekends and holidays.
-- Simplification: flows (qtrs > 0) are converted at the period-end rate, not
-- at the period's average rate.
CREATE VIEW v_facts_current_usd AS
SELECT f.*,
       CASE WHEN f.uom = 'USD' THEN f.value
            ELSE round(f.value * fx.usd_per_unit, 4) END AS value_usd,
       CASE WHEN f.uom = 'USD' THEN NULL ELSE fx.date END AS fx_date
FROM v_facts_current f
ASOF LEFT JOIN fx_usd_daily fx ON fx.currency = f.uom AND f.ddate >= fx.date
WHERE f.uom = 'USD' OR fx.usd_per_unit IS NOT NULL;
"""


def build(data_dir: Path, target: Path) -> Path:
    """Build a complete warehouse into `target` (which must not exist yet)."""
    if target.exists():
        target.unlink()
    con = duckdb.connect(str(target))
    try:
        con.execute("SET TimeZone = 'UTC'")
        _register_staging(con, data_dir)
        con.execute(SCHEMA_SQL)
        counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                  for t in COLUMN_LINEAGE}
        log.info("built %s: %s", target.name, counts)
    finally:
        con.close()
    return target


def _paths(data_dir: Path) -> tuple[Path, Path, Path]:
    out_dir = data_dir / "warehouse"
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = WAREHOUSE_FILE.removesuffix(".duckdb")
    return (out_dir / WAREHOUSE_FILE,
            out_dir / f"{stem}.building.duckdb",
            out_dir / f"{stem}.failed.duckdb")


def build_candidate(data_dir: Path) -> Path:
    """Build a complete warehouse next to the live one, without touching it."""
    _, candidate, _ = _paths(data_dir)
    return build(data_dir, candidate)


def promote(data_dir: Path) -> Path:
    """Replace the live warehouse with the candidate. Atomic on one filesystem."""
    live, candidate, failed = _paths(data_dir)
    os.replace(candidate, live)
    if failed.exists():
        failed.unlink()
    log.info("published %s", live)
    return live


def reject(data_dir: Path) -> Path:
    """Keep a failed build for inspection; the live warehouse stays as it was."""
    _, candidate, failed = _paths(data_dir)
    os.replace(candidate, failed)
    log.error("quality gate failed; live warehouse unchanged, failed build kept at %s", failed)
    return failed


def publish(data_dir: Path, run_gate) -> Path:
    """Build, gate, and only then replace the live warehouse.
    `run_gate(path)` must raise if the build is not fit to publish."""
    candidate = build_candidate(data_dir)
    try:
        run_gate(candidate)
    except Exception:
        reject(data_dir)
        raise
    return promote(data_dir)


def main() -> None:
    import argparse
    from src.quality.checks import evaluate

    parser = argparse.ArgumentParser(description="Build the warehouse and publish it if it passes the quality gate.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    publish(args.data_dir, lambda p: evaluate(p, args.data_dir, COLUMN_LINEAGE))


if __name__ == "__main__":
    main()
