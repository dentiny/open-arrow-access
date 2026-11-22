# Provider Arrow smoke read

Run from this repository checkout. Build once:

```sh
cargo build -p open-arrow-access-service --features arxiv,bhl --example provider_arrow_smoke
```

Read an arXiv source TAR from a local bulk mirror. The root contains `src/`:

```sh
target/debug/examples/provider_arrow_smoke arxiv /path/to/bulk/root src/arXiv_src_2610_008.tar
```

To read the requester-pays AWS distribution, set the usual AWS credential
environment variables and use `aws` as the root:

```sh
target/debug/examples/provider_arrow_smoke arxiv aws src/arXiv_src_2610_008.tar
```

Both paths fetch complete OAI metadata online. LaTeX content is selected without
attachments, and every selected article is consumed through EOF. PDF-only,
corrupt or otherwise unsupported articles fail the run. No recovery layer or
sampling limit hides those errors.

Read all OCR pages under a BHL prefix, including joined metadata:

```sh
target/debug/examples/provider_arrow_smoke bhl ocr/item-062861/item-062861-
```

Use the page-file prefix shown above. The parent directory also contains
`item-062861.txt`, a combined whole-item OCR file that is not a single page.
The example fails on invalid page identities instead of silently ignoring them.

This imports all 13 live public metadata tables once in memory. The compressed
tables total about 2.6 GiB; import memory and elapsed time are substantial. OCR
page selection is bounded by the prefix. If a database was already created with
`BhlMetadata::create`, reuse it:

```sh
target/debug/examples/provider_arrow_smoke bhl ocr/item-062861/item-062861- /path/to/metadata.duckdb
```

The example checks the public schema, stable batch schemas, full Arrow array
validation, required non-null columns, populated metadata and UTF-8 payloads.
BHL join warnings fail the run, and selected OCR keys must match output pages
exactly, with one row per page. Blank OCR pages are valid and counted separately.
`max_item_associations` exposes whether an item has retained multiple source
associations. A PASS line is printed only after EOF and all checks succeed.

Storage and OAI requests use the crate's existing transport policy: up to three
retries for retryable failures and a 60-second per-read timeout. These transport
retries do not skip any input or suppress schema, decoding or metadata errors.

A successful small sample validates only that sample. It does not establish that
every article in every production shard can be read, or that historical HTTP
406 responses have disappeared.

## Reuse one metadata database and run SQL

Create the database once with a 4 GB import memory budget and two DuckDB threads:

```sh
target/debug/examples/provider_arrow_smoke bhl-create /path/to/metadata.duckdb
```

For a local metadata mirror, supply its root as a third argument. It must contain
`data/title.txt.gz`, `data/item.txt.gz` and the other eleven standard table files.
Creation never overwrites an existing database.

Run ad hoc SQL and consume its Arrow results through EOF:

```sh
target/debug/examples/provider_arrow_smoke bhl-query /path/to/metadata.duckdb \
  'SELECT ItemID, TitleID FROM bhl_item WHERE __key = 62861 ORDER BY rowid'
```

The example displays at most 20 rows, validates every result array and counts all
rows. SQL results use DuckDB's reported Arrow schema. The same database can then
be passed to the `bhl` OCR command, avoiding another metadata import.

Rust callers can use the native connection directly:

```rust
let metadata = BhlMetadata::open("metadata.duckdb")?;
let connection = metadata.query_connection()?;
let mut query = connection.prepare(
    "SELECT ItemID, TitleID FROM bhl_item WHERE __key = ? ORDER BY rowid",
)?;
let rows = query.query_map([62861_u64], |row| {
    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
})?;
for row in rows {
    println!("{:?}", row?);
}
```

`query_connection()` also supports DuckDB's Arrow methods, as shown in the
example's `bhl-query` command. `BhlMetadata::create_with_config()` accepts native DuckDB
settings for import resource control; the ordinary `create()` keeps its defaults.
The example also sets a 4 GB query memory budget on its database connections.
For large live imports, optimized/release builds are recommended.
