"""Raw -> staging for one EDGAR quarter: typing, cleaning and quarantine.

Input  (from the ingest layer, every column a string):
    data/raw/<table>/year=YYYY/quarter=Q/*.parquet

Output:
    data/staging/<table>/year=YYYY/quarter=Q/     typed, cleaned, one row per key
    data/quarantine/<table>/year=YYYY/quarter=Q/  rejected rows, raw values + _reject_reason

Rules:
- Every column is trimmed; empty strings become NULL.
- Every typed column is parsed with try_* functions. A value that is present in
  raw but cannot be parsed is never silently turned into NULL: the row goes to
  quarantine with the reason, and keeps its raw values so it can be inspected.
- A missing required column is a schema change in the source, not a data
  problem: the job stops.
- Exact duplicates on the natural key are collapsed to one row. Duplicates that
  disagree on content cannot be resolved automatically: all of them go to
  quarantine as `conflicting_duplicate`.
- Facts whose filing (adsh) is not in the same quarter's staged filings are
  quarantined as `orphan_fact`.
- Row conservation is checked on every run:
      input = staged + quarantined + duplicates_dropped
  If it does not hold, the job fails rather than write a partial result.
- Re-running a quarter replaces that quarter's staging and quarantine partitions
  and nothing else.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("transform.edgar_staging")

ADSH_PATTERN = r"^\d{10}-\d{2}-\d{6}$"


# ---------------------------------------------------------------------------
# Column specifications
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Col:
    name: str
    kind: str = "string"      # string | long | int | date | timestamp | bool | decimal
    required: bool = False    # NULL after trimming -> quarantine


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Col, ...]
    key: tuple[str, ...]
    schema_required: tuple[str, ...] = field(default=())  # must exist in raw, else fail

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]


SUB = TableSpec(
    name="sub",
    columns=(
        Col("adsh", required=True),
        Col("cik", "long", required=True),
        Col("name", required=True),
        Col("sic", "int"),
        Col("countryba"),
        Col("stprba"),
        Col("cityba"),
        Col("countryinc"),
        Col("form", required=True),
        Col("period", "date", required=True),
        Col("fy", "int"),
        Col("fp"),
        Col("fye"),
        Col("filed", "date", required=True),
        Col("accepted", "timestamp", required=True),
        Col("prevrpt", "bool"),
        Col("nciks", "int"),
    ),
    key=("adsh",),
    schema_required=("adsh", "cik", "name", "form", "period", "filed", "accepted"),
)

NUM = TableSpec(
    name="num",
    columns=(
        Col("adsh", required=True),
        Col("tag", required=True),
        Col("version", required=True),
        Col("ddate", "date", required=True),
        Col("qtrs", "int", required=True),
        Col("uom", required=True),
        Col("segments"),          # added to the data sets in 2024; absent in older quarters
        Col("coreg"),
        Col("value", "decimal", required=True),
        Col("footnote"),
    ),
    key=("adsh", "tag", "version", "ddate", "qtrs", "uom", "segments", "coreg"),
    schema_required=("adsh", "tag", "version", "ddate", "qtrs", "uom", "value"),
)

TAG = TableSpec(
    name="tag",
    columns=(
        Col("tag", required=True),
        Col("version", required=True),
        Col("custom", "bool"),
        Col("abstract", "bool"),
        Col("datatype"),
        Col("iord"),
        Col("crdr"),
        Col("tlabel"),
        Col("doc"),
    ),
    key=("tag", "version"),
    schema_required=("tag", "version"),
)

SPECS = {s.name: s for s in (SUB, NUM, TAG)}
LINEAGE = ("_source_file", "_ingested_at")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _clean(name: str) -> Column:
    """Trim, and turn empty strings into NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _parse(col: Col, raw: Column) -> Column:
    if col.kind == "string":
        return raw
    if col.kind == "long":
        return F.expr(f"try_cast(`_c_{col.name}` as bigint)")
    if col.kind == "int":
        return F.expr(f"try_cast(`_c_{col.name}` as int)")
    if col.kind == "decimal":
        # DECIMAL(28,4): wide enough for any reported amount, and never a float.
        return F.expr(f"try_cast(`_c_{col.name}` as decimal(28,4))")
    if col.kind == "date":
        return F.expr(f"try_to_timestamp(`_c_{col.name}`, 'yyyyMMdd')").cast("date")
    if col.kind == "timestamp":
        # EDGAR writes e.g. "2024-02-02 16:05:12.0"; drop the fractional part.
        return F.expr(
            f"try_to_timestamp(regexp_replace(`_c_{col.name}`, '\\\\.\\\\d+$', ''), "
            f"'yyyy-MM-dd HH:mm:ss')"
        )
    if col.kind == "bool":
        return (F.when(raw == "1", F.lit(True))
                 .when(raw == "0", F.lit(False))
                 .otherwise(F.lit(None).cast("boolean")))
    raise ValueError(f"unknown kind {col.kind!r} for column {col.name}")


def _reject_reason(spec: TableSpec) -> Column:
    """First failing check wins, so every rejected row has exactly one reason."""
    checks: list[tuple[Column, str]] = []
    for col in spec.columns:
        raw, typed = F.col(f"_c_{col.name}"), F.col(col.name)
        if col.required:
            checks.append((raw.isNull(), f"missing_{col.name}"))
        if col.kind != "string":
            checks.append((raw.isNotNull() & typed.isNull(), f"unparseable_{col.name}"))
    if spec.name == "sub":
        checks.append((~F.col("adsh").rlike(ADSH_PATTERN), "malformed_adsh"))
    if spec.name == "num":
        checks.append((F.col("qtrs") < 0, "negative_qtrs"))

    reason = F.lit(None).cast("string")
    for condition, label in reversed(checks):
        reason = F.when(condition, F.lit(label)).otherwise(reason)
    return reason


def _typed(df: DataFrame, spec: TableSpec) -> DataFrame:
    missing = [c for c in spec.schema_required if c not in df.columns]
    if missing:
        raise ValueError(f"{spec.name}: required columns missing from raw data: {missing}")

    for col in spec.columns:  # optional columns absent in older quarters -> NULL
        if col.name not in df.columns:
            df = df.withColumn(col.name, F.lit(None).cast("string"))

    df = df.select(
        *[F.col(c).alias(f"_raw_{c}") for c in spec.names],
        *[_clean(c).alias(f"_c_{c}") for c in spec.names],
        *LINEAGE,
    )
    df = df.select("*", *[_parse(c, F.col(f"_c_{c.name}")).alias(c.name) for c in spec.columns])

    if spec.name == "sub":
        df = (df.withColumn("is_amendment", F.col("form").endswith("/A"))
                .withColumn("base_form", F.regexp_replace("form", "/A$", "")))
    return df.withColumn("_reject_reason", _reject_reason(spec))


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _dedupe(df: DataFrame, spec: TableSpec) -> tuple[DataFrame, DataFrame, int]:
    """Returns (kept, conflicting, duplicates_dropped)."""
    content_cols = [c for c in spec.names if c not in spec.key]
    content = F.sha2(F.concat_ws("\u001f", *[
        F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in content_cols
    ]), 256) if content_cols else F.lit("")

    by_key = Window.partitionBy(*spec.key)
    df = (df.withColumn("_content", content)
            .withColumn("_n_versions", F.size(F.collect_set("_content").over(by_key))))

    conflicting = (df.filter(F.col("_n_versions") > 1)
                     .withColumn("_reject_reason", F.lit("conflicting_duplicate")))
    clean = df.filter(F.col("_n_versions") == 1)

    ranked = clean.withColumn(
        "_rn", F.row_number().over(by_key.orderBy("_source_file", "_ingested_at")))
    kept = ranked.filter(F.col("_rn") == 1).drop("_rn")
    dropped = clean.count() - kept.count()
    return kept.drop("_content", "_n_versions"), conflicting.drop("_content", "_n_versions"), dropped


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _write_partition(df: DataFrame, root: Path, table: str, year: int, quarter: int) -> Path:
    """Replace exactly one partition. Removing the directory first matters: if a
    rerun produces zero rows, the old files must not survive."""
    part = root / table / f"year={year}" / f"quarter={quarter}"
    if part.exists():
        shutil.rmtree(part)
    df.coalesce(1).write.mode("overwrite").parquet(str(part))
    return part


@dataclass
class StageResult:
    table: str
    input_rows: int
    staged: int
    quarantined: int
    duplicates_dropped: int
    reasons: dict[str, int]

    def check_conservation(self) -> None:
        total = self.staged + self.quarantined + self.duplicates_dropped
        if total != self.input_rows:
            raise RuntimeError(
                f"{self.table}: row conservation failed: input={self.input_rows}, "
                f"staged+quarantined+dropped={total}")


def stage_table(spark: SparkSession, spec: TableSpec, raw_root: Path, out_root: Path,
                year: int, quarter: int, valid_adsh: DataFrame | None = None) -> StageResult:
    src = raw_root / spec.name / f"year={year}" / f"quarter={quarter}"
    if not src.exists():
        raise FileNotFoundError(f"raw partition not found: {src}")

    raw = spark.read.parquet(str(src))
    input_rows = raw.count()
    base = _typed(raw, spec).cache()
    try:
        typed = base
        if valid_adsh is not None:  # facts must belong to a filing staged this quarter
            known = valid_adsh.select(F.col("adsh").alias("_known_adsh"),
                                      F.lit(True).alias("_is_known"))
            typed = (typed.join(known, typed["adsh"] == known["_known_adsh"], "left")
                          .withColumn("_reject_reason", F.coalesce(
                              "_reject_reason",
                              F.when(F.col("_is_known").isNull(), F.lit("orphan_fact"))))
                          .drop("_known_adsh", "_is_known"))

        failed = typed.filter(F.col("_reject_reason").isNotNull())
        passed = typed.filter(F.col("_reject_reason").isNull())
        kept, conflicting, dropped = _dedupe(passed, spec)
        rejected = failed.unionByName(conflicting)

        derived = ["is_amendment", "base_form"] if spec.name == "sub" else []
        staged_df = kept.select(
            *spec.names, *derived, "_source_file",
            F.expr("try_cast(_ingested_at as timestamp)").alias("_ingested_at"),
            F.current_timestamp().alias("_staged_at"),
        )
        quarantine_df = rejected.select(
            *[F.col(f"_raw_{c}").alias(c) for c in spec.names], *LINEAGE, "_reject_reason")

        staged_path = _write_partition(staged_df, out_root / "staging", spec.name, year, quarter)
        quarantine_path = _write_partition(quarantine_df, out_root / "quarantine", spec.name, year, quarter)
    finally:
        base.unpersist()

    # Count what actually landed on disk, not what the in-memory plan says should have.
    staged_rows = spark.read.parquet(str(staged_path)).count()
    reasons = {r["_reject_reason"]: r["count"] for r in
               spark.read.parquet(str(quarantine_path)).groupBy("_reject_reason").count().collect()}
    result = StageResult(spec.name, input_rows, staged_rows, sum(reasons.values()), dropped, reasons)
    result.check_conservation()
    log.info("staged %s %dQ%d: %s", spec.name, year, quarter, result)
    return result


def stage_quarter(spark: SparkSession, data_dir: Path, year: int, quarter: int) -> dict[str, StageResult]:
    """Filings first, because facts are checked against them."""
    raw_root = data_dir / "raw"
    results = {
        "sub": stage_table(spark, SUB, raw_root, data_dir, year, quarter),
        "tag": stage_table(spark, TAG, raw_root, data_dir, year, quarter),
    }
    staged_sub = spark.read.parquet(str(data_dir / "staging" / "sub" / f"year={year}" / f"quarter={quarter}"))
    results["num"] = stage_table(spark, NUM, raw_root, data_dir, year, quarter,
                                 valid_adsh=staged_sub.select("adsh"))
    return results


def main() -> None:
    from src.transform.spark import get_spark

    parser = argparse.ArgumentParser(description="Stage one EDGAR quarter (raw -> staging).")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--quarter", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spark = get_spark("edgar-staging")
    try:
        for table, res in stage_quarter(spark, args.data_dir, args.year, args.quarter).items():
            log.info("%s: input=%d staged=%d quarantined=%d dropped=%d reasons=%s", table,
                     res.input_rows, res.staged, res.quarantined, res.duplicates_dropped, res.reasons)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
