# CourtListener bulk import

[ingest-courtlistener.sql](ingest-courtlistener.sql) creates **and populates**
`opinions`, `opinion_clusters`, `dockets` and `courts` in a new DuckDB file.
It reads the public HTTPS `.csv.bz2` exports directly, using `cache_httpfs` for cached HTTP
and the community `compression_fs` extension for bzip2 decompression.

Tested with DuckDB 2.0.0. Run from the repository root against a new file:

```sh
duckdb -bail -storage-version v1.5.0 /path/to/courtlistener.duckdb \
  < metadata_import/courtlistener/ingest-courtlistener.sql
```

The script force-installs both extensions from community, loads them and creates
the four explicit typed tables. It inserts rows through `read_csv()`, rejects duplicate IDs and checkpoints
the database. No Rust importer or second SQL file is needed. The storage-version
flag keeps the file readable by the library's DuckDB 1.5.x binding.

The default export date is `2026-09-30`. Edit `courtlistener_export_date` and
`courtlistener_bulk_root` near the top to select another reviewed snapshot or a
local mirror with the same `<table>-<date>.csv.bz2` filenames. The DDL pins all
132 source fields, their types and nullable flags; review headers before changing
the date. For an uncompressed local mirror, remove `.bz2` from the macro's suffix
and the compression_fs INSTALL/LOAD statements.

The SQL reads every source column as text, then INSERT casts into the explicit
table types and enforces NOT NULL constraints. Backslash quote escaping,
multiline text, unquoted NULL and quoted empty strings are preserved. The signed
`citation_count` column preserves negative source values (including `-1` in the
2026-09-30 export). Native DuckDB cast rules apply; this is not the provider's
stricter lexical validator.
The CSV reader keeps `parallel = true`; `preserve_insertion_order` and
`row_number() OVER ()` retain zero-based logical record ordinals in
`__courtlistener_ordinal`. Forcing `parallel = false` caused CSV reader buffers
to accumulate and exhausted a 12 GB limit on the full 2026-09-30 cluster export,
even with a single DuckDB thread. Keep the default parallel scan path enabled. The 64 MiB line/buffer limit is
explicit and may need increasing for larger legitimate records; parse errors
fail instead of dropping rows.

TEXT/HTML/XML remain inline. PDF paths/URLs remain references; no PDF download
occurs. Source exports are full snapshots and may contain missing foreign-key
matches. They are not delta files or an atomic cross-table snapshot.

Use the generic `DuckDbDatabase::open(path)` to query the resulting tables.
This standalone script does not write the versioned `courtlistener_snapshot`
header required by the separate prepared-reader workflow in
[PR #727](https://github.com/dentiny/open-arrow-access/pull/727), matching the
standalone-import distinction documented for BHL. Run once with `-bail`; a failed
import can leave previously committed tables, so rebuild a new database on retry.
