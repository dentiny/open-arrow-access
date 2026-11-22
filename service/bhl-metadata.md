# BHL metadata joins with embedded DuckDB

Metadata is included with `service-bhl` (service-crate feature `bhl`).
The implementation uses the bundled Rust DuckDB binding, supporting 1.20000.1
through 1.20001.x; the lockfile selects 1.20001.0. These bindings use Arrow 60
internally. Each joined metadata batch is serialized through Arrow IPC and
imported into the public Arrow 58 schema, adding an in-memory serialization step.
OCR payload bytes do not pass through this conversion.

The first version has three steps:

1. `BhlMetadata::load(storage).await` imports all 13 AWS `data/*.txt.gz` tables
   concurrently into one in-memory DuckDB database. Each table has its own
   connection. The import futures run concurrently with async source I/O; only
   synchronous DuckDB batch writes and table finalization use blocking workers.
   The existing gzip/TSV reader validates headers and preserves lexical strings.
2. `for_each` executes one [join SQL](src/bhl/metadata/join.sql) for the requested
   page identities. DuckDB performs joins, parent resolution and ordered list
   aggregation, without multiplying independent 1:N relations.
3. Rust converts each result batch to the existing fixed Arrow metadata schema
   and immediately emits each page through the callback. It does not collect
   the full query result into a Rust lookup map.

```rust,ignore
use open_arrow_access_service::bhl::metadata::BhlMetadata;

let metadata = BhlMetadata::load(bucket_root_storage).await?;
metadata.for_each(&page_keys, |page| {
    // page.object: requested OCR object
    // page.identity: parsed page identity
    // page.metadata: one-row StructArray matching the existing 104-field schema
    // page.missing / page.conflicts: reconciliation messages for this page
    emit(page)
})?;
```

`for_each` is synchronous; async applications can run it on a blocking worker.
Callback errors stop emission immediately. Rows follow request order, include
their original object/identity, and preserve repeated requests. Each query has
a separate connection and request table. Use `BhlPageAccessor::from_metadata`
to combine these results with OCR bytes as shown below. Lance writing remains the caller's responsibility.

The in-memory `load` path assumes sufficient memory for the loaded tables and
query intermediates. Prepared metadata can instead use the persistent database
described below. DuckDB uses its default resource settings, with
`enable_external_file_cache=false` explicitly set.

`metadata.item` and `metadata.title` are now non-null lists, retaining every
item/title association in source order while emitting one OCR row per request.
Consumers of the previous singleton structs must accept these list fields.
The 13 source tables and 104 lexical source columns remain unchanged:
 preserve original lexical
IDs, empty strings, quotes, Unicode, relation duplicates/order, and separate
item/part rights. DOI matches entity type and ID. Part page sequences use
partpage. Invalid headers/keys, missing tables and title/page/part numeric collisions
fail loading. Missing joins and parent/sequence conflicts accompany each emitted
page rather than silently dropping it. Callers decide which rows to publish.

`info` and `schema_metadata()` retain source paths, row counts and decoded-cell
SHA-256 fingerprints. They do not identify an atomic AWS snapshot or compressed
object versions. Use pinned storage when reproducible acquisition is required.

## Read OCR pages with joined metadata

```rust,ignore
use std::sync::Arc;
use open_arrow_access_core::Accessor;
use open_arrow_access_service::{FileSetRequest, bhl::{BhlPageAccessor, metadata::BhlMetadata}};

let metadata = Arc::new(BhlMetadata::load(metadata_storage).await?);
let pages = BhlPageAccessor::from_metadata(
    ocr_storage,
    snapshot_id,
    metadata, // Reuse this loaded value across jobs/readers.
    |page| {
        // Inspect/store page.missing and page.conflicts here.
        // Ok(()) retains this page; Err stops the scan before its OCR read.
        Ok(())
    },
)?;
let mut reader = pages.open(FileSetRequest::new(page_keys)).await?;
while let Some(batch) = reader.next().await? {
    emit(batch)?;
}
```

The reader runs one join SQL for the entire request when consumption begins.
Each selected metadata row becomes one independent OCR partition under the
shared scan executor. One-row batches retain the fixed nine-column page schema,
original binary/empty OCR bytes, caller byte I/O options, source object identity
and snapshot ID. Metadata fingerprints/source counts are attached automatically;
`with_schema_metadata` merges additional caller provenance. The existing
`BhlPageAccessor::new` still accepts a custom metadata provider.

On first read, a blocking worker executes the synchronous DuckDB query and
sends joined rows through a one-page bounded queue. OCR partitions can start
before all joined rows have been converted; the async executor remains free.
The reader retains the selected keys and DuckDB's query state/current Arrow
batch, but no longer collects every joined metadata row in a Rust Vec. Loaded
source tables and SQL execution can still require substantial memory.
Dropping discovery closes the queue and stops emission at the next send; it
does not interrupt a DuckDB operation already in progress. Worker errors and
panics propagate after any queued page, so consume through EOF before committing
output.

Query failures propagate through the reader. Report callback failures stop
before that page's OCR read. Accepted pages with missing joins retain null
children and still emit their OCR record. Reports go to the caller callback;
they are not accumulated into schema metadata.

Sequential reads follow request order, including duplicates. The scan remains
reorderable for callers explicitly using unordered/parallel execution layers.
Projection constructs only selected page columns. Requests without `data`
perform no OCR object opens/downloads; zero-column projections still retain one
row per selected page. Selecting `data` keeps its original bytes and I/O options.
The loaded source tables and request-level DuckDB join remain unchanged.

Metadata import sends decoded Arrow batches directly through DuckDB's Arrow
appender. Each table keeps one appender and explicitly flushes once at EOF.
It retains lexical-row SHA-256 hashing and source order; no source rows are
filtered or reinterpreted. Source tables store only the lexical columns, so
decoded batches are appended without adding key or ordinal arrays. Views expose
the table's `rowid` for ordering and calculate numeric join keys from the original
ID columns with `bhl_id`. This removes per-cell owned String/Value creation,
per-row appender calls, and per-batch appender recreation/flush. Each table
uses one blocking importer while its asynchronous byte reader runs on the
existing runtime; all 13 import tasks are joined before returning.

Request-level page selections also use Arrow appends, in batches of up to 8,192
identities. Request ordinals retain input order and duplicate selections; the
selection appender flushes once before the join query runs.

## Reuse one persistent metadata database across page jobs

A coordinator imports the 13 TSV tables directly into a persistent DuckDB
file. Independent processes open that file read-only. Per-query page selections
use connection-local temporary tables, without a separate in-memory database,
source-data copy or forwarding views.

```rust,ignore
BhlMetadata::create(storage.clone(), "bhl-metadata.duckdb").await?;
// Distribute this exact file to the workers, or place it on a shared filesystem.
let metadata = Arc::new(BhlMetadata::open("bhl-metadata.duckdb")?);
let pages = BhlPageAccessor::from_metadata(storage, snapshot_id, metadata, report)?;
```

`create` imports into a temporary database on the destination filesystem,
commits data/provenance, checkpoints and closes the database, then atomically
publishes the file without overwriting an existing path. Workers directly open
it with DuckDB's `READ_ONLY` access mode. `open` validates the format
version, all source table schemas and row counts, and retains source fingerprints.

Source freshness, acquisition consistency, storage and distribution are
caller-owned. Use a compatible DuckDB storage version. Existing `load` behavior
remains an in-memory import; conversion-service job wiring remains a consumer
adoption step after this API is released.
