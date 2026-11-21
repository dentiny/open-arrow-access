# BHL metadata joins with embedded DuckDB

Metadata is included with `service-bhl` (service-crate feature `bhl`).
The implementation uses the bundled Rust DuckDB
binding, on the 1.10506 patch line to match the workspace's Arrow 58 types.

The first version has three steps:

1. `BhlMetadata::load(storage).await` imports all 13 AWS `data/*.txt.gz` tables
   concurrently into one in-memory DuckDB database. Each table has its own
   connection and blocking worker; the existing gzip/TSV reader handles source
   I/O, exact headers and lexical string preservation.
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
Callback errors stop emission immediately. Query order is unspecified; each
row includes its original object/identity, and repeated requests remain repeated
rows. Each query has a separate connection and request table. OCR acquisition
and Lance writing remain the caller's responsibility; the emitted StructArray
is compatible with `BhlPageAccessor`/`BhlPageRecord`.

V1 assumes sufficient memory for the loaded tables and query intermediates.
There are no memory-limit options, disk-backed database, checkpoint, temporary
workspace or temporary-directory settings. DuckDB uses its default resource
settings, with `enable_external_file_cache=false` explicitly set.

The fixed schema and source semantics are unchanged: preserve original lexical
IDs, empty strings, quotes, Unicode, relation duplicates/order, and separate
item/part rights. DOI matches entity type and ID. Part page sequences use
partpage. Invalid headers/keys, missing tables and singleton numeric collisions
fail loading. Missing joins and parent/sequence conflicts accompany each emitted
page rather than silently dropping it. Callers decide which rows to publish.

`info` and `schema_metadata()` retain source paths, row counts and decoded-cell
SHA-256 fingerprints. They do not identify an atomic AWS snapshot or compressed
object versions. Use pinned storage when reproducible acquisition is required.
