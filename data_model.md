# Data model

## Warehouse (DuckDB, `data/warehouse/edgar.duckdb`)

### fact_financial_fact

| Column | Source | Notes |
|---|---|---|
| filing_sk | dim_filing | the filing that reported the value |
| filer_sk | dim_filer | company version valid at the filing's acceptance time |
| tag_sk | dim_tag | tag + taxonomy version |
| date_sk | dim_date | from `ddate` (yyyymmdd) |
| adsh | num.adsh | degenerate dimension (accession number) |
| qtrs | num.qtrs | period length in quarters; 0 = point in time (balance sheet) |
| uom | num.uom | unit of measure |
| segments, coreg | num | dimensional qualifiers; NULL = consolidated entity |
| value | num.value | DECIMAL(28,4), never float |
| known_at | sub.accepted | when the value became public |

**Grain:** one row per staged fact: `adsh, tag, version, ddate, qtrs, uom, segments, coreg`.

### dim_filer (SCD Type 2)

Business key `cik`. Tracked attributes: `name`, `sic`, `countryba`, `stprba`.
A new version starts at the acceptance time of the first filing whose tracked
attributes differ from the company's previous filing. `valid_to` is the next
version's `valid_from`; NULL on the current version. Facts are joined to the
version valid at their filing's acceptance time.

### dim_tag (Type 1)

Key `tag` + `version`. Type 1 is sufficient because the taxonomy version is part
of the key: a changed definition arrives as a new key. Tags used in `num` but
absent from `tag` get their own row with `in_tag_table = FALSE`, so distinct
undescribed tags are never merged.

### dim_filing

One row per filing, copied from `staging.filing_versions` (below) with a
surrogate key.

### dim_date

One row per day between the earliest and latest date referenced;
`is_quarter_end` flag.

### facts_as_of(ts) and v_facts_current

For each fact identity (`cik, tag, date, qtrs, uom, segments, coreg`), the value
from the filing with the latest `known_at <= ts`. `v_facts_current` is
`facts_as_of` at the end of time.

## Staging tables (implemented)

### staging.sub / staging.num / staging.tag

Typed copies of the raw tables, one row per natural key, partitioned by
`year` / `quarter`, with lineage columns `_source_file`, `_ingested_at`,
`_staged_at`. `staging.sub` adds `is_amendment` and `base_form`
(`10-K/A` -> `10-K`).

| Table | Natural key |
|---|---|
| sub | `adsh` |
| num | `adsh, tag, version, ddate, qtrs, uom, segments, coreg` |
| tag | `tag, version` |

### staging.filing_versions

One row per filing. A *family* is all filings with the same
`cik, base_form, period`; versions are ordered by `accepted`.

| Column | Meaning |
|---|---|
| `family_id` | hash of the family key |
| `version_no`, `n_versions`, `is_latest` | position in the family |
| `supersedes_adsh`, `superseded_by_adsh` | previous / next version |
| `valid_from`, `valid_to` | acceptance time of this and of the next version; `valid_to` NULL = current |
| `original_not_loaded` | family starts with an amendment: the original is outside the loaded quarters |
| `amendment_not_loaded` | SEC's `prevrpt` says the filing was amended, but no later version is loaded |
| `duplicate_original` | more than one non-amended filing for the same period |

## Decisions

- **Amended filings:** keep every version and link them; do not overwrite.
  Reports choose `is_latest` or an as-of date through `valid_from` / `valid_to`.
- **Same fact reported in several filings** (e.g. prior-year comparatives in a
  later 10-K): every report is kept as its own fact row; `facts_as_of` picks the
  most recently published one at the requested time.
