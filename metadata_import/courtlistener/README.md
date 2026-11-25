# CourtListener normalized bulk import

[ingest-courtlistener.sql](ingest-courtlistener.sql) is one complete **CREATE +
IMPORT** script for every reviewed snapshot. All dates produce the same four
source tables, column order, types and nullability: normalized schema version 1.
Only input configuration and recorded source-field availability vary by date.

Use DuckDB 2.0.1 and run from the repository root against a new file:

```sh
duckdb -bail -storage-version v1.5.0 /path/to/courtlistener.duckdb \
  < metadata_import/courtlistener/ingest-courtlistener.sql
```

The script force-installs and loads `cache_httpfs` and `compression_fs` from
community, reads public HTTPS `.csv.bz2` exports, creates and populates the tables,
checks unique IDs, records completion and checkpoints. No Rust importer is
required. The storage-version flag keeps the file readable by DuckDB 1.5.x.

## One schema for all dates

The fixed schema includes the union of **all 132 CSV source fields**:
`opinions` (22), `opinion_clusters` (36), `dockets` (54) and `courts` (20).
Each table also has `__courtlistener_ordinal UBIGINT NOT NULL`. No exported field
is discarded, and columns found only in the companion PostgreSQL schema are not
invented as exported data.

A field is nullable if any reviewed snapshot lacks it or permits NULL. Compared
with the strict 2026-09-30 source schema, these previously required fields become
nullable:

| Table | Fields |
| --- | --- |
| `opinions` | `xml_scan` |
| `opinion_clusters` | `filepath_json_harvard`, `filepath_pdf_harvard`, `arguments`, `headmatter` |
| `dockets` | `filepath_local`, `view_count`, `federal_dn_case_type`, `federal_dn_office_code`, `federal_dn_judge_initials_assigned`, `federal_dn_judge_initials_referred`, `docket_number_raw`, `docket_number_source` |
| `courts` | `pacer_rss_entry_types`, `notes` |

`INSERT ... BY NAME` fills unavailable historical columns with **NULL**, never
empty strings or zeroes. Source integer counters use signed `BIGINT` (`page_count`, vote counts,
`citation_count`, `view_count`, `ia_upload_failure_count`), preserving the source
SQL integer range. The real 2026-09-30 export contains `citation_count = -1`.
Only generated ordinals and row counts use unsigned integers. Existing nullable
fields remain nullable.
The complete fixed types and constraints are explicit in the SQL declarations.

`courtlistener_import_manifest` records schema version, snapshot date, each
source URI, `exported_columns`, `missing_columns`, CSV dialect, completion and
row count. This distinguishes an unavailable field from an exported field whose
value is NULL. A complete database has four manifest rows with `completed=true`.

```sql
SELECT schema_version, snapshot_date, table_name, missing_columns, row_count
FROM courtlistener_import_manifest
ORDER BY table_name;
```

The normalized tables have wider nullability and signed source counters,
compared with the strict latest-CSV Arrow contract in `CourtListenerSchemas::default()`. DuckDB-based conversion derives
its Arrow schema from the normalized tables and can use the same query for every
date. The direct raw-CSV accessor still requires a matching source schema/dialect.

## Snapshot and CSV compatibility

Edit `courtlistener_export_date` near the top. The script accepts 36 reviewed,
nonempty four-table dates from `2022-08-02` through `2026-09-30`; the complete list
is checked in the SQL. The upstream empty `2024-03-01` export is excluded. Review
headers and dialect before adding another date.

Input dialect is selected from the reviewed date: early files use doubled double
quotes, August–December 2024 files use doubled backticks, and 2025 onward files
use double quotes with backslash escaping. The July 2023 `SET` preamble is skipped.
Actual source headers are checked against the reviewed column list before any
source table is populated. These input differences never change the output DDL.

Edit `courtlistener_bulk_root` for a local mirror with the same filenames.
For an uncompressed mirror, replace `.csv.bz2` with `.csv` in the SQL.
Unquoted NULL, quoted empty strings and inline multiline TEXT/HTML/XML are
preserved. Native DuckDB cast rules apply, rather than the provider's stricter
lexical validator. PDF/file references stay references; import does not download
external documents.

The reader keeps `parallel=true`: forcing the sequential CSV path retained
buffers and exhausted a 12 GB limit on the full cluster export. Keep the default
scan path even when using one DuckDB thread. `preserve_insertion_order` and
`row_number() OVER ()` retain logical record ordinals; downstream ordered reads
use `ORDER BY __courtlistener_ordinal`. The explicit 64 MiB line/buffer limit may
need increasing for larger legitimate records; parse failures never drop rows.

Snapshots overlap and are not delta files or an atomic cross-table export.
Run once with `-bail`: failures can leave earlier committed tables and manifest
rows, so a simple rerun should use a new database. The manifest is distinct from
PR #727's `courtlistener_snapshot` prepared-reader header. Use the generic
`DuckDbDatabase::open(path)` for these standalone normalized tables.

Validation: one importer SQL was exercised on record-aligned real samples for
all 36 dates (144 source tables), checking identical physical schema, original
ordinals, nonempty text values and NULLs for absent fields. Sample validation
does not assert full-date import completion.
