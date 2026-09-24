"""Pipeline steps, each tracked with OpenLineage, and a runner for all of them.

The same step functions are used by the command line below and by the Airflow
DAGs in `dags/`, so a local run and a scheduled run execute identical code.

    python -m src.pipeline --quarters 2025q3 2025q4            # raw already landed
    python -m src.pipeline --quarters 2025q4 --download --fx   # download EDGAR and FX first

Steps
    edgar.ingest.<q>        download + raw landing           per quarter
    fx.rates                incremental ECB rates            daily
    edgar.staging.<q>       typing, cleaning, quarantine     per quarter
    edgar.amendments        link amended filings             all quarters
    edgar.warehouse         build the DuckDB star schema into a candidate file
    edgar.quality_gate      checks; publish on pass, keep previous warehouse on fail
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import asdict
from pathlib import Path

import duckdb

from src.lineage.openlineage import Emitter, dataset, track
from src.quality import checks as quality
from src.warehouse.build import COLUMN_LINEAGE, build_candidate, promote, reject

log = logging.getLogger("pipeline")


def parse_quarter(text: str) -> tuple[int, int]:
    year, q = text.lower().split("q")
    return int(year), int(q)


def _emitter(data_dir: Path, emitter: Emitter | None) -> Emitter:
    return emitter or Emitter(data_dir)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def step_ingest(data_dir: Path, year: int, quarter: int, emitter: Emitter | None = None) -> dict:
    from src.ingest.edgar import download, land

    user_agent = os.environ.get("SEC_USER_AGENT")
    if not user_agent:
        raise RuntimeError("Set SEC_USER_AGENT (see .env.example)")
    with track(_emitter(data_dir, emitter), f"edgar.ingest.{year}q{quarter}",
               inputs=[dataset(f"{year}q{quarter}.zip", namespace="https://www.sec.gov")]) as r:
        counts = land(download(year, quarter, data_dir / "archives", user_agent),
                      data_dir / "raw", year, quarter)
        r.outputs = [dataset(f"raw.{t}", row_count=n) for t, n in counts.items()]
    return counts


def step_fx(data_dir: Path, emitter: Emitter | None = None, **kwargs) -> dict:
    from src.ingest.fx import load_incremental

    with track(_emitter(data_dir, emitter), "fx.rates",
               inputs=[dataset("EXR", namespace="https://data-api.ecb.europa.eu")]) as r:
        result = load_incremental(data_dir, **kwargs)
        r.outputs = [dataset("staging.fx_rates", row_count=result.inserted + result.updated)]
    return asdict(result)


def step_stage(data_dir: Path, year: int, quarter: int, emitter: Emitter | None = None,
               spark=None) -> dict:
    from src.transform.edgar_staging import stage_quarter

    with _spark(spark) as s, track(_emitter(data_dir, emitter), f"edgar.staging.{year}q{quarter}",
                                   inputs=[dataset(f"raw.{t}") for t in ("sub", "num", "tag")]) as r:
        res = stage_quarter(s, data_dir, year, quarter)
        r.outputs = ([dataset(f"staging.{t}", row_count=x.staged) for t, x in res.items()]
                     + [dataset(f"quarantine.{t}", row_count=x.quarantined) for t, x in res.items()])
    return {t: asdict(x) for t, x in res.items()}


def step_amendments(data_dir: Path, emitter: Emitter | None = None, spark=None) -> dict:
    from src.transform.amendments import resolve_amendments

    with _spark(spark) as s, track(_emitter(data_dir, emitter), "edgar.amendments",
                                   inputs=[dataset("staging.sub")]) as r:
        stats = resolve_amendments(s, data_dir)
        r.outputs = [dataset("staging.filing_versions", row_count=stats["filings"])]
    return stats


def step_warehouse(data_dir: Path, emitter: Emitter | None = None) -> str:
    """Build a candidate, gate it, and publish it. Raises QualityGateError on failure."""
    emitter = _emitter(data_dir, emitter)
    inputs = [dataset(f"staging.{t}") for t in ("sub", "num", "tag", "filing_versions", "fx_rates")]
    with track(emitter, "edgar.warehouse", inputs=inputs) as r:
        candidate = build_candidate(data_dir)
        r.outputs = _warehouse_outputs(candidate)

    with track(emitter, "edgar.quality_gate", inputs=[]) as r:
        try:
            results = quality.evaluate(candidate, data_dir, COLUMN_LINEAGE)
        except quality.QualityGateError as err:
            r.inputs = _assertions(err.results)
            reject(data_dir)
            raise
        r.inputs = _assertions(results)
        return str(promote(data_dir))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _spark:
    """Use the given Spark session, or create one and stop it afterwards."""

    def __init__(self, session):
        self.session, self.own = session, session is None

    def __enter__(self):
        if self.own:
            from src.transform.spark import get_spark
            self.session = get_spark("edgar-pipeline")
        return self.session

    def __exit__(self, *exc):
        if self.own:
            self.session.stop()


def _warehouse_outputs(db: Path) -> list[dict]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        out = []
        for table, lineage in COLUMN_LINEAGE.items():
            cols = con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position", [table]).fetchall()
            rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            out.append(dataset(f"warehouse.{table}", fields=dict(cols),
                               column_lineage=lineage, row_count=rows))
        return out
    finally:
        con.close()


def _assertions(results: list[quality.CheckResult]) -> list[dict]:
    by_table: dict[str, list[dict]] = {}
    for r in results:
        by_table.setdefault(r.table, []).append(
            {"assertion": r.name, "success": r.passed, "column": r.column})
    return [dataset(t if "." in t or t == "warehouse" else f"warehouse.{t}", assertions=a)
            for t, a in by_table.items()]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(data_dir: Path, quarters: list[tuple[int, int]], download: bool = False,
        fx: bool = False, emitter: Emitter | None = None, spark=None) -> Path:
    emitter = _emitter(data_dir, emitter)
    if download:
        for y, q in quarters:
            step_ingest(data_dir, y, q, emitter)
    if fx:
        step_fx(data_dir, emitter)
    with _spark(spark) as s:
        for y, q in quarters:
            step_stage(data_dir, y, q, emitter, spark=s)
        step_amendments(data_dir, emitter, spark=s)
    return Path(step_warehouse(data_dir, emitter))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the EDGAR pipeline end to end.")
    parser.add_argument("--quarters", nargs="+", required=True, help="e.g. 2025q3 2025q4")
    parser.add_argument("--download", action="store_true", help="download and land EDGAR data first")
    parser.add_argument("--fx", action="store_true", help="load ECB FX rates incrementally first")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    live = run(args.data_dir, [parse_quarter(q) for q in args.quarters], args.download, args.fx)
    log.info("warehouse ready: %s", live)


if __name__ == "__main__":
    main()
