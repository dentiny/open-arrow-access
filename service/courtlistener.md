# CourtListener bulk snapshots

## Service and data

CourtListener provides legal data including courts, opinions and opinion
clusters. Its bulk export stores dated table snapshots as bzip2-compressed CSV,
with companion SQL schema objects. This provider discovers snapshots and decodes
a selected table's bytes.

Enable facade feature `service-courtlistener`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## File hierarchy and dataset boundaries

The following paths are concrete examples or explicitly marked templates, not
an exhaustive live inventory. This section describes the selected upstream
representation; it does not add automatic corpus discovery or parsing.

```text
s3://com-courtlistener-storage/
  bulk-data/
    courts-<YYYY-MM-DD>.csv.bz2
    dockets-<YYYY-MM-DD>.csv.bz2
    opinion-clusters-<YYYY-MM-DD>.csv.bz2
    opinions-<YYYY-MM-DD>.csv.bz2
    schema-<YYYY-MM-DD>.sql
  embeddings/opinions/<model>/...     # separate dataset, not CSV table shards
```

Filenames identify table and UTC generation date. Discover actual available objects
rather than presuming every table shares one date. Same-date tables are exported while
the database is live, so cross-table references can be missing. Keep schema and export
dialect with the selected files. Courts, dockets, clusters and opinions represent
different entities, not interchangeable document partitions.

## Sharding strategy and current library boundary

`CourtListenerBulkAccessor` discovers dated objects and opens one sequential decoded CSV byte stream. It advertises no range or parallel-byte capability. `records(format)` composes a caller-supplied record format with `FileSetAccessor`; the caller must provide the table schema and correct CSV dialect. Independent table objects can have independent decoder state, but differing table schemas must not be forced into one strict file-set schema.

| Boundary | Meaning |
| --- | --- |
| Acquisition | One dated table CSV.bz2 and its associated schema. |
| Logical grouping | Table + generation date + primary key. |
| Proposed shard | Whole files initially; parsed primary-key buckets for very large tables. |
| Unsafe boundary | Arbitrary bzip2 ranges or newline splitting of CSV containing multiline text. |

Recommended application plan (not a new provider request option):

Choose explicit table/date pairs and freeze object digests. Stream decompression into a
parser configured for PostgreSQL COPY CSV semantics, including header, double quotes,
backslash escapes, embedded newlines and NULL versus quoted empty strings. Do not assume
a generic CSV default is sufficient. Preserve tables independently with their own
schemas and primary/foreign keys. For a large table, stage parsed records into buckets
using SHA-256 of the UTF-8 primary-key string, first eight bytes unsigned big-endian
modulo fixed N. Persist table, date, N and membership. This creates downstream shards
after sequential framing; it does not parallelize initial bzip2 decompression.

Pin a manifest containing exact source keys/URLs, logical identities, source
version/date, representation, byte sizes and downloaded-byte digests, parser/schema
version and exact shard membership. Keep unknown metadata and original bytes
where a typed projection cannot preserve them. Archive transport chunks, Arrow
batches and output fragments do not define upstream dataset shards.

For independent record files, compose an explicit record decoder/schema with
file-set planning and the [parallel execution layer](../docs/parallel-read.md).
A file set defaults to one partition per file; native file planning is an
additional opt-in when the inner format supports it. Independent compressed files
can use separate decoder state. This does not imply parallel decompression or
range-readable records within a single compressed stream. Bound active readers,
parser buffers and queued output; parallelism is not a process memory cap.
Apply one compatible schema per scan and preserve source identities independently
of batch arrival order.

## Snapshot refresh and validation

A newer dated table replaces that table's prior full snapshot; appending it duplicates
unchanged rows. Keep historical snapshots separately if required. Validate and report
cross-table orphans instead of silently dropping them. Embeddings have separate
model/version and inventory semantics and should have a separate ingestion plan.

Reconcile parsed rows and primary-key uniqueness per table. Verify NULL/empty
round-trips and a multiline/escaped-text sample. Report foreign-key orphans as
snapshot-consistency findings, and confirm every selected source record is in exactly
one staged shard.

These are design recommendations based on the existing provider source and
publisher references. No live corpus conversion, full inventory acquisition or
sharding performance measurement was performed for this documentation.

- [Bulk data documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
- [Bulk object browser](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/)

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

Source: [CourtListener bulk data](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data).
