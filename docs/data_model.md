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

## Open questions

- Amended filings (`form` = 10-K/A, 10-Q/A): keep both and flag, or supersede?
- Duplicate facts across filings for the same period: which one is authoritative?
