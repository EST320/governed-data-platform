"""Amended-filing handling: link every filing to the versions before and after it.

A company that finds an error in a 10-K files a 10-K/A for the same period. The
amendment does not replace the original in the source data; both are there,
filed months apart. Picking one and discarding the other loses information
either way:

- keep only the original  -> reports use numbers the company has corrected;
- keep only the amendment -> history is rewritten: a report run "as of" a date
  before the amendment was filed would show numbers nobody could see then.

So nothing is discarded. Filings are grouped into families (same company, same
report type, same period) and ordered by acceptance time. Each version gets a
validity interval:

    valid_from = its own acceptance time
    valid_to   = acceptance time of the next version (NULL while it is current)

"The latest numbers" and "the numbers as known on date D" then become two
filters on the same table.

Input:  data/staging/sub/  (all quarters)
Output: data/staging/filing_versions/  (whole table, rebuilt on every run)
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("transform.amendments")

FAMILY_KEY = ("cik", "base_form", "period")


def build_filing_versions(filings: DataFrame) -> DataFrame:
    # A filing belongs to exactly one quarter's archive. If the same accession
    # number shows up twice, keep the earliest-landed copy.
    first_copy = Window.partitionBy("adsh").orderBy("year", "quarter", "_source_file")
    filings = (filings.withColumn("_copy", F.row_number().over(first_copy))
                      .filter(F.col("_copy") == 1).drop("_copy"))

    family = Window.partitionBy(*FAMILY_KEY)
    ordered = family.orderBy("accepted", "adsh")  # adsh breaks ties deterministically

    return (
        filings
        .withColumn("family_id", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in FAMILY_KEY]), 256).substr(1, 16))
        .withColumn("version_no", F.row_number().over(ordered))
        .withColumn("n_versions", F.count("*").over(family))
        .withColumn("is_latest", F.col("version_no") == F.col("n_versions"))
        .withColumn("supersedes_adsh", F.lag("adsh").over(ordered))
        .withColumn("superseded_by_adsh", F.lead("adsh").over(ordered))
        .withColumn("valid_from", F.col("accepted"))
        .withColumn("valid_to", F.lead("accepted").over(ordered))
        # Consistency flags. They do not block anything; they make gaps visible.
        .withColumn("original_not_loaded",  # the family starts with an amendment
                    (F.col("version_no") == 1) & F.col("is_amendment"))
        .withColumn("amendment_not_loaded",  # SEC says this was amended, but we have no later version
                    F.coalesce(F.col("prevrpt"), F.lit(False)) & F.col("superseded_by_adsh").isNull())
        .withColumn("duplicate_original",  # more than one non-amended filing for the same period
                    F.sum(F.when(~F.col("is_amendment"), 1).otherwise(0)).over(family) > 1)
        .select(
            "family_id", "adsh", "cik", "name", "form", "base_form", "is_amendment",
            "period", "fy", "fp", "filed", "accepted",
            "version_no", "n_versions", "is_latest",
            "supersedes_adsh", "superseded_by_adsh", "valid_from", "valid_to",
            "original_not_loaded", "amendment_not_loaded", "duplicate_original",
            "prevrpt", "year", "quarter", "_source_file",
        )
    )


def resolve_amendments(spark: SparkSession, data_dir: Path) -> dict[str, int]:
    src = data_dir / "staging" / "sub"
    if not src.exists():
        raise FileNotFoundError(f"no staged filings under {src}; run edgar_staging first")

    versions = build_filing_versions(spark.read.parquet(str(src))).cache()
    try:
        out = data_dir / "staging" / "filing_versions"
        tmp = out.with_name(out.name + "._tmp")
        if tmp.exists():
            shutil.rmtree(tmp)
        versions.coalesce(1).write.parquet(str(tmp))
        if out.exists():  # swap in the new table only after it is fully written
            shutil.rmtree(out)
        tmp.rename(out)

        stats = versions.agg(
            F.count("*").alias("filings"),
            F.countDistinct("family_id").alias("families"),
            F.sum(F.col("is_amendment").cast("int")).alias("amendments"),
            F.sum(F.col("original_not_loaded").cast("int")).alias("original_not_loaded"),
            F.sum(F.col("amendment_not_loaded").cast("int")).alias("amendment_not_loaded"),
            F.sum(F.col("duplicate_original").cast("int")).alias("duplicate_original"),
        ).first().asDict()
    finally:
        versions.unpersist()

    stats = {k: int(v or 0) for k, v in stats.items()}
    log.info("filing versions: %s", stats)
    return stats


def main() -> None:
    from src.transform.spark import get_spark

    parser = argparse.ArgumentParser(description="Link amended filings to their originals.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("edgar-amendments")
    try:
        resolve_amendments(spark, args.data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
