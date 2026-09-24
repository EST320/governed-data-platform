# Governed Data Platform

[![CI](https://github.com/EST320/governed-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/EST320/governed-data-platform/actions/workflows/ci.yml)

An end-to-end data platform on SEC financial statement data: ingestion, Spark
transformation, a DuckDB star schema with point-in-time queries, a blocking
quality gate, and OpenLineage events with column-level lineage.

**Status:** ingestion, transformation, warehouse, quality gate and lineage implemented and tested. Orchestration next.

---

## Why this project

Most portfolio pipelines stop at "the data moved." The harder questions come after that:

- **Can you prove a load was complete?** Every step checks that rows in = rows out + rows rejected, and fails if not.
- **Can you show where a number came from?** Every warehouse column declares its source columns; the lineage is emitted as OpenLineage and a missing declaration fails the build.
- **Does a bad load get stopped before anyone reads it?** The warehouse is built next to the live one and only replaces it after passing the quality gate.
- **What did we know, and when?** Companies amend their reports. Overwriting the old numbers silently rewrites history; this platform keeps every version and can answer "what was the reported value as of date D".

The last question is what point-in-time data is for: a backtest that uses a
restated number before the restatement was published has look-ahead bias,
and its results look better than anything that could have been traded.

## Data

**SEC EDGAR Financial Statement Data Sets**: quarterly archives published by
the U.S. Securities and Exchange Commission. No API key, no registration.

| File | Contents | Used for |
|---|---|---|
| `sub.txt` | one row per filing: company, form, period, acceptance time | `dim_filer` (SCD2), `dim_filing` |
| `num.txt` | numeric facts per filing, tag and period | `fact_financial_fact` |
| `tag.txt` | XBRL tag definitions | `dim_tag` |

Twelve quarters is tens of millions of fact rows, and the data is dirty in
realistic ways: amended filings, the same figure reported in several filings,
malformed values, custom tags without definitions.

## Architecture

```mermaid
flowchart LR
    A[SEC EDGAR<br/>quarterly ZIP] -->|ingest| B[(raw<br/>Parquet)]
    B -->|PySpark| C[(staging)]
    B -->|PySpark| Q[(quarantine)]
    C -->|amendments| V[(filing<br/>versions)]
    C --> W[(DuckDB<br/>candidate)]
    V --> W
    W --> G{quality<br/>gate}
    G -->|pass| P[(DuckDB<br/>live)]
    G -->|fail| F[(failed build<br/>kept for inspection)]
    B & C & W & G -.OpenLineage.-> L[events.jsonl /<br/>Marquez]
```

## Layers

| Layer | Path | Contents |
|---|---|---|
| raw | `data/raw/<table>/year=YYYY/quarter=Q/` | source as landed, every column a string |
| staging | `data/staging/<table>/year=YYYY/quarter=Q/` | typed, trimmed, one row per natural key |
| quarantine | `data/quarantine/<table>/year=YYYY/quarter=Q/` | rejected rows, raw values plus `_reject_reason` |
| filing versions | `data/staging/filing_versions/` | every filing linked to its earlier and later versions |
| warehouse | `data/warehouse/edgar.duckdb` | star schema, published only after the quality gate |
| quality log | `data/warehouse/quality_log.jsonl` | every check of every run, including rejected builds |
| lineage | `data/lineage/events.jsonl` | OpenLineage events for every step |

## Warehouse

Kimball star schema in DuckDB. Full column list in [docs/data_model.md](docs/data_model.md).

| Table | Grain | Notes |
|---|---|---|
| `fact_financial_fact` | one staged fact: filing × tag × date × period length × unit × dimension | `value` is `DECIMAL(28,4)`; `known_at` is when the filing became public |
| `dim_filer` | company × attribute version | **SCD Type 2** on name, SIC code, state, country |
| `dim_filing` | filing | version chain: `version_no`, `is_latest`, `valid_from`, `valid_to` |
| `dim_tag` | tag × taxonomy version | Type 1; tags used but never described get their own flagged row |
| `dim_date` | day | quarter-end flag |

**Point-in-time queries**

```sql
-- Revenue as the market could see it on 1 March 2024
SELECT * FROM facts_as_of(TIMESTAMP '2024-03-01') WHERE tag = 'Revenues';

-- Latest known values
SELECT * FROM v_facts_current WHERE cik = 320193;
```

For every fact (company, concept, period, unit, dimension), `facts_as_of(ts)`
returns the value from the most recently accepted filing that was already
public at `ts`. This covers amendments and the more common case of a figure
being restated as a comparative in the following year's report.

## Quality gate

Each check is a SQL query that counts offending rows. `error` checks block
publication; `warn` checks are recorded and reported.

| Check | Severity |
|---|---|
| fact grain unique; every fact resolves to a company version, a tag and a date; no NULL values | error |
| fact rows reconcile exactly to staged rows | error |
| SCD2: one current version per company; intervals valid and contiguous | error |
| one latest version per filing family | error |
| every warehouse column has declared lineage | error |
| quarantine rate below 5% (error) / 1% (warn) | error / warn |
| balance sheet balances: Assets = Liabilities + Equity | warn |
| facts whose tag has no definition; amendments referenced but not loaded | warn |

## Lineage

Every step emits OpenLineage `START` then `COMPLETE` or `FAIL`, with input and
output datasets. Outputs carry schema and row counts; warehouse tables carry
column-level lineage; the quality gate reports its checks as data quality
assertions. Events always go to `data/lineage/events.jsonl`, and also to a
lineage backend when `OPENLINEAGE_URL` is set:

```bash
export OPENLINEAGE_URL=http://localhost:5000   # a local Marquez
```

If the backend is unreachable the pipeline continues and logs a warning.

## Quick start

```bash
git clone https://github.com/EST320/governed-data-platform.git
cd governed-data-platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# SEC asks automated clients to identify themselves
export SEC_USER_AGENT="Your Name you@example.com"

# everything: download, stage, link amendments, build, gate, publish
python -m src.pipeline --quarters 2025q3 2025q4 --download

pytest
```

Individual steps can also be run on their own: `src.ingest.edgar`,
`src.transform.edgar_staging`, `src.transform.amendments`, `src.warehouse.build`.

Spark needs Java 17 or later. On Windows, use WSL: Spark's local file writes on
native Windows need extra Hadoop binaries.

## Stack

| Layer | Choice | Status |
|---|---|---|
| Ingestion | Python, requests, pandas → Parquet | Done |
| Processing | PySpark (local mode) | Done |
| Warehouse | DuckDB | Done |
| Quality | SQL checks as a blocking gate | Done |
| Lineage | OpenLineage (file + HTTP to Marquez) | Done |
| Orchestration | Apache Airflow | Planned |
| Runtime | Docker Compose | Planned |

## Design decisions

**Raw means raw.** The ingestion layer lands every column as a string and adds
two lineage columns, `_source_file` and `_ingested_at`. Typing and cleaning
belong in the transform layer, where they can be tested and changed without
re-downloading anything.

**Idempotent by partition.** Re-running a quarter replaces that quarter's
partitions and nothing else.

**Downloads are atomic.** Archives are written to a `.part` file and renamed
only when complete, so an interrupted download never looks like a finished one.

**Bad values go to quarantine, never to NULL.** Parsing uses `try_cast` and
`try_to_timestamp`. A value that is present but cannot be parsed sends the row
to quarantine with a reason such as `unparseable_value` or `orphan_fact`, raw
values intact. Turning it into NULL would make a data problem look like a
missing value.

**Every run proves it lost nothing.** Staging checks
`input = staged + quarantined + duplicates_dropped`, counted from the files
actually written; the warehouse checks fact rows against staged rows. This
check caught a real bug during development: a cached DataFrame from a previous
run was being reused, and the run reported a quarantined row that was no longer
in the source.

**Duplicates that disagree are not resolved by guessing.** Identical rows
collapse to one. Rows with the same key but different content all go to
quarantine as `conflicting_duplicate`.

**Amendments are versions, not overwrites.** A 10-K/A does not replace the
original 10-K. Filings are grouped by company, report type and period, ordered
by acceptance time, and each version gets `valid_from` / `valid_to`.

**Build next to live, publish only after the gate.** The warehouse is built
into a separate file. Only a build that passes every error-level check replaces
the live file, with an atomic rename. A rejected build is kept as
`edgar.failed.duckdb`, and its check results stay in the quality log.

**SQL checks rather than Great Expectations.** The checks run inside DuckDB on
full tables with no data copied into Python, and each one is a query that can be
pasted into a console to list the offending rows. The cost is no generated
documentation site; `quality_results` and the JSON log take its place.

**Lineage is declared, and the declaration is enforced.** `COLUMN_LINEAGE` in
`src/warehouse/build.py` maps each warehouse column to its staging sources. It
is what gets emitted to OpenLineage, and the quality gate fails if a warehouse
column has no entry, so lineage cannot silently fall behind the schema.

**DuckDB rather than Postgres or a cloud warehouse.** The project has to be
reproducible by someone with five minutes and no cloud account. DuckDB is a
single file, needs no server, and is columnar.

## Roadmap

- [x] Ingestion: quarterly archive download, retry, atomic write, idempotent raw landing
- [x] Unit tests and CI
- [x] Transformation: PySpark typing, cleaning, quarantine, amended-filing handling
- [x] Warehouse: DuckDB star schema, SCD2 `dim_filer`, point-in-time queries
- [x] Quality: blocking gate with recorded results
- [x] Lineage: OpenLineage events with column-level lineage
- [ ] Orchestration: Airflow DAG with retries and SLA alerting
- [ ] Incremental source: daily FX rates, demonstrating incremental load

---

Last updated: 2026-09-24
