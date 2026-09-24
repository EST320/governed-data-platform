"""Download SEC EDGAR Financial Statement Data Sets and land them as Parquet.

Raw layout (Hive-style partitions, one file per table per quarter):

    data/raw/<table>/year=YYYY/quarter=Q/part-0.parquet

Design choices:
- Idempotent: re-running a quarter overwrites that partition only, so a rerun
  never duplicates rows and never touches other quarters.
- Raw means raw: every column is landed as string. Typing and cleaning belong
  to the transform layer, where they can be tested and changed without
  re-downloading anything.
- Each landed file gets two lineage columns, _source_file and _ingested_at,
  so any row can be traced back to the archive it came from.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

log = logging.getLogger("ingest.edgar")

BASE_URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets"
TABLES = ("sub", "num", "tag")


def quarter_name(year: int, quarter: int) -> str:
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"quarter must be 1-4, got {quarter}")
    if year < 2009:
        raise ValueError("EDGAR financial statement data sets start in 2009")
    return f"{year}q{quarter}"


def download(year: int, quarter: int, dest_dir: Path, user_agent: str,
             retries: int = 3, backoff: float = 5.0) -> Path:
    """Download one quarterly archive. Skips the download if the file exists."""
    name = quarter_name(year, quarter)
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"{name}.zip"
    if target.exists() and zipfile.is_zipfile(target):
        log.info("archive %s already present, skipping download", target.name)
        return target

    url = f"{BASE_URL}/{name}.zip"
    headers = {"User-Agent": user_agent}
    for attempt in range(1, retries + 1):
        try:
            log.info("downloading %s (attempt %d/%d)", url, attempt, retries)
            with requests.get(url, headers=headers, stream=True, timeout=120) as r:
                r.raise_for_status()
                tmp = target.with_suffix(".part")
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
            tmp.replace(target)  # atomic: a half-written file never looks complete
            return target
        except requests.RequestException as exc:
            log.warning("download failed: %s", exc)
            if attempt == retries:
                raise
            time.sleep(backoff * attempt)
    raise RuntimeError("unreachable")


def land(archive: Path, raw_root: Path, year: int, quarter: int) -> dict[str, int]:
    """Extract sub/num/tag from an archive and write them as Parquet partitions.

    Returns row counts per table, which the caller can log or check.
    """
    counts: dict[str, int] = {}
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with zipfile.ZipFile(archive) as zf:
        members = set(zf.namelist())
        for table in TABLES:
            member = f"{table}.txt"
            if member not in members:
                raise FileNotFoundError(f"{member} missing from {archive.name}")
            with zf.open(member) as fh:
                df = pd.read_csv(
                    fh, sep="\t", dtype=str, keep_default_na=False,
                    encoding="utf-8", encoding_errors="replace",
                    quoting=3,  # csv.QUOTE_NONE: EDGAR text fields contain stray quotes
                )
            df["_source_file"] = f"{archive.name}/{member}"
            df["_ingested_at"] = ingested_at

            part_dir = raw_root / table / f"year={year}" / f"quarter={quarter}"
            part_dir.mkdir(parents=True, exist_ok=True)
            for old in part_dir.glob("*.parquet"):
                old.unlink()  # overwrite the partition, nothing else
            df.to_parquet(part_dir / "part-0.parquet", index=False)
            counts[table] = len(df)
            log.info("landed %s %dQ%d: %d rows", table, year, quarter, len(df))
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest one EDGAR quarter.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--quarter", type=int, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    user_agent = os.environ.get("SEC_USER_AGENT")
    if not user_agent:
        raise SystemExit("Set SEC_USER_AGENT, e.g. 'Your Name you@example.com' (see .env.example)")

    archive = download(args.year, args.quarter, args.data_dir / "archives", user_agent)
    counts = land(archive, args.data_dir / "raw", args.year, args.quarter)
    log.info("done: %s", counts)


if __name__ == "__main__":
    main()
