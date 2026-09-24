"""A small but realistic two-quarter dataset, and a helper that lands it as raw."""
from pathlib import Path

import pandas as pd

LINEAGE = {"_source_file": "fixture.zip", "_ingested_at": "2024-07-01T00:00:00+00:00"}

SUB_COLS = ["adsh", "cik", "name", "sic", "form", "period", "fy", "fp", "filed", "accepted", "prevrpt"]
NUM_COLS = ["adsh", "tag", "version", "ddate", "qtrs", "uom", "value"]
TAG_COLS = ["tag", "version", "custom", "abstract", "datatype", "tlabel"]

ACME_10K = "0000000001-24-000001"
ACME_10KA = "0000000001-24-000010"
ACME_10Q = "0000000001-24-000020"
BETA_10K = "0000000002-24-000001"
V = "us-gaap/2023"

QUARTERS = {
    (2024, 1): {
        "sub": [
            (ACME_10K, "1", "ACME CORP", "1000", "10-K", "20231231", "2023", "FY", "20240215", "2024-02-15 17:00:00.0", "1"),
            (BETA_10K, "2", "BETA INC", "2000", "10-K", "20231231", "2023", "FY", "20240220", "2024-02-20 17:00:00.0", "0"),
        ],
        "num": [
            (ACME_10K, "Revenues", V, "20231231", "4", "USD", "100"),
            (ACME_10K, "Assets", V, "20231231", "0", "USD", "500"),
            (ACME_10K, "LiabilitiesAndStockholdersEquity", V, "20231231", "0", "USD", "500"),
            (BETA_10K, "Revenues", V, "20231231", "4", "USD", "200"),
        ],
        "tag": [
            ("Revenues", V, "0", "0", "monetary", "Revenues"),
            ("Assets", V, "0", "0", "monetary", "Assets"),
            ("LiabilitiesAndStockholdersEquity", V, "0", "0", "monetary", "Liabilities and Equity"),
        ],
    },
    (2024, 2): {
        "sub": [
            # ACME renames itself and amends its 10-K: revenue restated from 100 to 110
            (ACME_10KA, "1", "ACME HOLDINGS", "1000", "10-K/A", "20231231", "2023", "FY", "20240420", "2024-04-20 09:00:00.0", "0"),
            (ACME_10Q, "1", "ACME HOLDINGS", "1000", "10-Q", "20240331", "2024", "Q1", "20240505", "2024-05-05 09:00:00.0", "0"),
        ],
        "num": [
            (ACME_10KA, "Revenues", V, "20231231", "4", "USD", "110"),
            (ACME_10Q, "Revenues", V, "20240331", "1", "USD", "30"),
            (ACME_10Q, "CustomMetric", "0000000001-24-000020", "20240331", "1", "USD", "7"),  # tag not in tag table
        ],
        "tag": [
            ("Revenues", V, "0", "0", "monetary", "Revenues"),
        ],
    },
}

COLS = {"sub": SUB_COLS, "num": NUM_COLS, "tag": TAG_COLS}


def land_fixture(data_dir: Path) -> None:
    for (year, quarter), tables in QUARTERS.items():
        for table, rows in tables.items():
            df = pd.DataFrame(rows, columns=COLS[table])
            for k, v in LINEAGE.items():
                df[k] = v
            part = data_dir / "raw" / table / f"year={year}" / f"quarter={quarter}"
            part.mkdir(parents=True, exist_ok=True)
            df.to_parquet(part / "part-0.parquet", index=False)
