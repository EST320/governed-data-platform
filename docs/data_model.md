# Data model

Target: Kimball star schema in DuckDB.

## fact_financial_fact

| Column | Source | Notes |
|---|---|---|
| filer_sk | dim_filer | surrogate key, resolved as of the filing date |
| tag_sk | dim_tag | surrogate key |
| date_sk | dim_date | from `ddate` |
| adsh | num.adsh | degenerate dimension (filing accession number) |
| qtrs | num.qtrs | period length in quarters; 0 = point in time |
| uom | num.uom | unit of measure |
| value | num.value | DECIMAL(28,4) — never float |

**Grain:** one row per adsh × tag × version × ddate × qtrs × uom.

## dim_filer (SCD Type 2)

Business key: `cik`. Tracked attributes: `name`, `sic`, `countryba`, `stprba`.
Columns `valid_from`, `valid_to`, `is_current`.

## dim_tag (SCD Type 2)

Business key: `tag` + `version`.

## dim_date

Standard date dimension with fiscal quarter and fiscal year flags.

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
  later 10-K): kept per filing; the fact table resolves which one applies
  through the filing's version interval. To be implemented in the warehouse layer.
