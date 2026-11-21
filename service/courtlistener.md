# CourtListener bulk snapshots

## Service and data

CourtListener provides legal data including courts, opinions and opinion
clusters. Its bulk export stores dated table snapshots as bzip2-compressed CSV,
with companion SQL schema objects. This provider discovers snapshots and decodes
a selected table's bytes.

Enable facade feature `service-courtlistener`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Configure byte storage rooted above `bulk-data/`, then use
`CourtListenerBulkAccessor::new(storage)`.

| Request | Response |
| --- | --- |
| `snapshots(table)` | Sorted, unique `Vec<CourtListenerSnapshot>`; requires listing |
| `CourtListenerSnapshot::new(table, "YYYY-MM-DD")` | Validated selector, no I/O |
| `open(ByteReadRequest::new(snapshot.object()))` | Sequential decoded CSV `ByteReader`; explicit reads do not require listing |

`object()` is `bulk-data/{table}-{date}.csv.bz2`; `schema_object()` identifies
`bulk-data/schema-{date}.sql` without fetching it. Input byte options are forwarded.
Decoded content length is unknown and ranges are unsupported. CSV quoting,
backslash escapes, multiline values and NULL/empty distinctions remain intact.
The provider defines no Arrow schema. Same-date tables are not an atomic database
snapshot. For public S3, callers configure the `com-courtlistener-storage` bucket,
region and unsigned access on their OpenDAL operator; local mirrors work too.

## Typed table records

`CourtListenerTableAccessor` provides strict typed records for `opinions`,
`opinion-clusters`, `dockets` and `courts`. `CourtListenerSchemas::default()` pins
the 22/36/54/20 exported fields and SQL nullability of the 2026-09-30 snapshot.
Use `table_schema(table)` to inspect a flat table schema, and `schemas.schema()`
for the nested opinion/cluster/docket/court output contract described in
[the date-shard design](https://github.com/dentiny/open-arrow-access/pull/617).

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, Result};
use open_arrow_access::service::courtlistener::CourtListenerTableAccessor;
use open_arrow_access::storage::OpenDalAccessor;

async fn read_opinions(storage: OpenDalAccessor) -> Result<()> {
    let source = CourtListenerTableAccessor::new(storage);
    let mut reader = source.open(ReadRequest::new(
        "bulk-data/opinions-2026-09-30.csv.bz2",
    )).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} opinions", batch.num_rows());
    }
    Ok(())
}
```

Timestamps become UTC microsecond values, dates become `Date32`, booleans and
numeric IDs/codes/counts use explicit numeric types, and text identifiers such
as court IDs retain leading zeros. The parser distinguishes unquoted NULL from
quoted empty strings and supports COPY backslash escaping and multiline fields.
Invalid values (including negative counts), required NULLs and header drift
fail rather than becoming null or losing rows. `with_dialect` accepts an explicit
`CourtListenerCopyDialect` for reviewed historical exports (for example backtick
quotes); quote/escape rules are never guessed from the file. All fields are validated even
when projected away. EOF/errors are terminal; no background producer is spawned.

Historical header/type variants require an explicit reviewed `with_schemas`
contract; this reader does not download or execute source SQL. Input storage
still supplies the compressed bytes. `with_max_record_bytes` defaults to 64 MiB
per CSV record, including the header, and prevents unbounded record buffering.
Batches target 8 MiB and at most the requested row count (capped at 1024); one
oversized permitted row may exceed that target. These are not process memory
limits. `ScanPlanner` exposes one lazy partition per input object, suitable for
explicit file-set composition. It does not split one bzip2 file into row ranges,
join source tables, discover output destinations or write a Lance dataset.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ByteReadRequest, Result,
    service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot},
    storage::{OpenDalAccessor, opendal},
};

async fn read_table(operator: opendal::Operator, table: &str, date: &str) -> Result<()> {
    let snapshots = CourtListenerBulkAccessor::new(OpenDalAccessor::new(operator));
    let selected = CourtListenerSnapshot::new(table, date)?;
    println!(
        "CSV: {}, SQL: {}",
        selected.object().locator(),
        selected.schema_object().locator()
    );
    let mut reader = snapshots
        .open(ByteReadRequest::new(selected.object()))
        .await?;
    if let Some(bytes) = reader.next().await? {
        println!("decoded CSV chunk: {} bytes", bytes.len());
    }
    Ok(())
}
```

For a table such as `courts`, the example identifies the CSV/SQL objects and
reads decompressed CSV bytes. A chunk is not a row. Use a consumer matching the
export's SQL/CSV semantics before turning these bytes into Arrow columns.

Source: [CourtListener bulk data](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data).
