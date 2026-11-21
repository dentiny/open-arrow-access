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

V1 assumes sufficient memory for the loaded tables and query intermediates.
There are no memory-limit options, disk-backed database, checkpoint, temporary
workspace or temporary-directory settings. DuckDB uses its default resource
settings, with `enable_external_file_cache=false` explicitly set.

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

On first read, the caller's thread executes the synchronous DuckDB query and
buffers metadata for the selected request in a Vec. OCR partitions then consume
those rows. The reader uses no query worker, channel or `spawn_blocking`; the
SQL call occupies the current thread until it finishes. V1 assumes enough
memory for the request's metadata as well as the loaded source tables.

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
It retains lexical-row SHA-256 hashing and source ordinals; no source rows are
filtered or reinterpreted. This removes per-cell owned String/Value creation,
per-row appender calls, and per-batch appender recreation/flush. Each table
uses one blocking importer while its asynchronous byte reader runs on the
existing runtime; all 13 import tasks are joined before returning.

Request-level page selections also use Arrow appends, in batches of up to 8,192
identities. Request ordinals retain input order and duplicate selections; the
selection appender flushes once before the join query runs.
