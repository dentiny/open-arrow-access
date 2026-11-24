# BHL metadata joins with embedded DuckDB

Metadata is included with `service-bhl` (service-crate feature `bhl`).
The implementation uses the bundled Rust DuckDB binding on the 1.10506.x line;
the lockfile selects 1.10506.1. This line uses Arrow 58, matching the public
workspace schema. The dependency range stays on that binding line so fresh
consumers do not select the incompatible Arrow 60 bindings. Arrow batches pass
directly between the importer/query and the public schema; OCR payload bytes
remain unchanged.

The first version has three steps:

1. `BhlMetadata::load(storage).await` imports all 13 AWS `data/*.txt.gz` tables
   concurrently into one in-memory DuckDB database. Each table has its own
   connection. The import futures run concurrently with async source I/O; only
   synchronous DuckDB batch writes and table finalization use blocking workers.
   The existing gzip/TSV reader validates headers and preserves lexical strings.
2. `for_each` executes one [join SQL](src/bhl/metadata/join.sql) for the requested
   page identities. DuckDB performs joins, parent resolution and ordered list
   aggregation, without multiplying independent 1:N relations.
   Before the correlated joins, connection-local temporary tables retain only
   the selected pages and their reachable metadata relations. Original source
   row ordinals, duplicates, missing targets and conflicting parents are retained.
   This reduces the input to join materialization; unprepared source tables may
   still require a scan to find those records. Existing databases need no rebuild.
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
`enable_external_file_cache=false` explicitly set. All three entry points
(`load`, `create`, and `open`) use a private system temporary directory for DuckDB
spill files, independent of the working directory or database-file permissions.
Set `TMPDIR` to select a writable scratch volume with sufficient disk space.
The directory is shared by cloned connections and removed after the last
connection closes, including when an import fails or its future is dropped.

`metadata.item`, `metadata.title`, `metadata.page` and `metadata.part` are now non-null lists, retaining every
item/title association, page-type record and part-parent association in source order while emitting one OCR row per request.
Consumers of the previous singleton structs must accept these list fields.
The 13 source tables and 104 lexical source columns remain unchanged:
 preserve original lexical
IDs, empty strings, quotes, Unicode, relation duplicates/order, and separate
item/part rights. DOI matches entity type and ID. Part page sequences use
partpage. Invalid headers/keys, missing tables and title numeric collisions
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
before that page's OCR read. Sequence selection is mandatory before reports.
Accepted pages with other missing joins retain
empty relationship lists and still emit their OCR record. Reports go to the caller callback;
they are not accumulated into schema metadata.

Sequential reads follow the selected keys' request order. Duplicate matches for
an entity/PageID fail instead of emitting repeated rows. The scan remains
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

`create` opens the requested file directly, imports all source tables, commits
its data/provenance, checkpoints and closes all write connections before returning.
The coordinator must await successful completion before starting readers on that
same file. There is no staging database or file publication step. Existing paths
are rejected; a failed import may leave an incomplete file that the caller must
discard before retrying. `open` requires the completed metadata header and validates
the format version, source table schemas and row counts. Workers open the file
with DuckDB's `READ_ONLY` access mode and retain source fingerprints.

Source freshness, acquisition consistency, storage and distribution are
caller-owned. Use a compatible DuckDB storage version. Existing `load` behavior
remains an in-memory import; conversion-service job wiring remains a consumer
adoption step after this API is released.

## Open an existing SQL-dumped DuckDB database

Use `open_tables` for a caller-created file containing the 13 source tables:

```rust,ignore
let metadata = Arc::new(BhlMetadata::open_tables(
    "/data/BHL-metadata.duckdb",
    "sha256:<database-file-sha256>",
)?);
let pages = BhlPageAccessor::from_metadata(storage, snapshot_id, metadata, report)?;
```

This opens the file with DuckDB's `READ_ONLY` mode. It does not acquire TSVs,
copy tables, calculate lexical-row checksums, or require the importer's
`__bhl_metadata` header and persistent `bhl_*` views. Required tables must retain
the BHL source columns in their original order, all as `VARCHAR`, preserving empty
strings. Column nullability constraints need not be declared in the dump; consumed
NULL cells fail rather than being silently converted to empty strings. Title IDs
must be valid, unique positive numeric keys.

Opening validates the table schemas and checks title keys.
Every query connection creates private temporary helper views/macros and page
selections. Neither opening nor querying modifies the database file. Concurrent
readers can share one `Arc<BhlMetadata>`.

The caller supplies an immutable database identity, preferably the file SHA-256
computed once when publishing it. This becomes the metadata fingerprint. External
files do not produce TSV source entries: `sources` is empty, with no per-table row
counts or fabricated lexical checksums. Managed databases created by `load`/`create`
retain their existing provenance and validation.

BHL reuses the shared DuckDB loader for local paths and remote URLs. For example, open an authenticated remote database:

```rust,ignore
use open_arrow_access_service::duckdb::DuckDbDatabase;

let database = DuckDbDatabase::connect(
    "s3://bucket/BHL-metadata.duckdb",
    Some("CREATE SECRET metadata (TYPE s3, PROVIDER credential_chain);"),
)?;
let metadata = BhlMetadata::from_tables_database(database, "published-snapshot-id")?;
```

The shared connection executes the SQL string before attaching the database
read-only. Remote reads use cache_httpfs with a temporary on-disk block cache,
without downloading a complete database before opening. The shared loader owns
credentials, connection cloning and cache lifetime. `BhlMetadata` only adds BHL
schema/key validation and per-query helpers. Each `DuckDbDatabase::connect`
force-installs cache_httpfs from community and loads it, including for local paths.
`BhlShardAccessor::from_tables_database(storage, path, snapshot_id)` remains a
local convenience wrapper. The existing `open`/`from_database` APIs continue to
validate importer-managed databases.

## OCR version selection

Metadata joins select the unique key matching metadata `SequenceOrder` in the
existing metadata join (`page` for items, `partpage` for parts). Other versions
are excluded and logged to stderr without reading their bodies. Missing or
ambiguous matches fail; filename sequences may be zero, but IDs remain positive.
This rule also applies to explicit file selections, before caller report callbacks.
