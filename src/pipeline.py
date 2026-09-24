"""Run the pipeline end to end, with a lineage event for every step.

    python -m src.pipeline --quarters 2025q3 2025q4            # raw already landed
    python -m src.pipeline --quarters 2025q4 --download        # download and land first

Steps
    edgar.ingest.<q>        download + raw landing           (only with --download)
    edgar.staging.<q>       typing, cleaning, quarantine     per quarter
    edgar.amendments        link amended filings             all quarters
    edgar.warehouse         build the DuckDB star schema
    edgar.quality_gate      checks; a failure keeps the previous warehouse live
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import duckdb

from src.lineage.openlineage import Emitter, dataset, track
from src.quality import checks as quality
from src.warehouse.build import COLUMN_LINEAGE, build_candidate, promote, reject

log = logging.getLogger("pipeline")


def parse_quarter(text: str) -> tuple[int, int]:
    year, q = text.lower().split("q")
    return int(year), int(q)


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


def run(data_dir: Path, quarters: list[tuple[int, int]], download: bool = False,
        emitter: Emitter | None = None, spark=None) -> Path:
    from src.transform.amendments import resolve_amendments
    from src.transform.edgar_staging import stage_quarter
    from src.transform.spark import get_spark

    emitter = emitter or Emitter(data_dir)

    if download:
        from src.ingest.edgar import download as fetch, land
        user_agent = os.environ.get("SEC_USER_AGENT")
        if not user_agent:
            raise SystemExit("Set SEC_USER_AGENT (see .env.example)")
        for y, q in quarters:
            with track(emitter, f"edgar.ingest.{y}q{q}",
                       inputs=[dataset(f"{y}q{q}.zip", namespace="https://www.sec.gov")]) as r:
                counts = land(fetch(y, q, data_dir / "archives", user_agent), data_dir / "raw", y, q)
                r.outputs = [dataset(f"raw.{t}", row_count=n) for t, n in counts.items()]

    own_spark = spark is None
    spark = spark or get_spark("edgar-pipeline")
    try:
        for y, q in quarters:
            with track(emitter, f"edgar.staging.{y}q{q}",
                       inputs=[dataset(f"raw.{t}") for t in ("sub", "num", "tag")]) as r:
                res = stage_quarter(spark, data_dir, y, q)
                r.outputs = ([dataset(f"staging.{t}", row_count=x.staged) for t, x in res.items()]
                             + [dataset(f"quarantine.{t}", row_count=x.quarantined) for t, x in res.items()])

        with track(emitter, "edgar.amendments", inputs=[dataset("staging.sub")]) as r:
            stats = resolve_amendments(spark, data_dir)
            r.outputs = [dataset("staging.filing_versions", row_count=stats["filings"])]
    finally:
        if own_spark:
            spark.stop()

    staging_inputs = [dataset(f"staging.{t}") for t in ("sub", "num", "tag", "filing_versions")]
    with track(emitter, "edgar.warehouse", inputs=staging_inputs) as r:
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
        return promote(data_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the EDGAR pipeline end to end.")
    parser.add_argument("--quarters", nargs="+", required=True, help="e.g. 2025q3 2025q4")
    parser.add_argument("--download", action="store_true", help="download and land raw data first")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    live = run(args.data_dir, [parse_quarter(q) for q in args.quarters], args.download)
    log.info("warehouse ready: %s", live)


if __name__ == "__main__":
    main()
