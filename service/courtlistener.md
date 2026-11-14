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
For nullable string columns named by the export's header, bind
`CsvFileFormat::from_header()` through `snapshots.records(format)`. This preserves
all-null columns and avoids a separate header read; enable `format-csv` when
using the facade and select files with `FileSetRequest`. Apply table-specific
SQL casts or an explicit schema when typed columns are needed.

Source: [CourtListener bulk data](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data).
