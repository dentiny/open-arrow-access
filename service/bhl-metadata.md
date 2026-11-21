# BHL metadata joins with embedded DuckDB

Enable `service-bhl-metadata`. This implementation is based on `duckdb-rs` and
uses bundled DuckDB; no DuckDB executable, server, Python runtime or runtime
extension installation is required. The binding is kept on its 1.10506 patch
line because that line shares the workspace's Arrow 58 types.

## Read, join and supply page metadata

```rust,ignore
use open_arrow_access_service::bhl::{BhlPageAccessor, metadata::BhlMetadata};

// Both accessors are rooted at the bucket (or a mirror with identical keys).
let metadata = BhlMetadata::load(metadata_storage).await?;
let selected = metadata.select(&page_keys)?;
// Inspect selected.report before deciding whether this conversion can publish.
let provenance = selected.schema_metadata()?;
let pages = BhlPageAccessor::new(
    ocr_storage,
    snapshot_id,
    move |identity| selected.get(identity),
)?
.with_schema_metadata(provenance);
```

`load` reads all 13 `data/*.txt.gz` objects through the existing streaming
gzip/TSV reader. It validates exact headers and positive numeric primary/join
keys, hashes the original decoded lexical cells and appends each batch into
DuckDB. It does not retain Rust source-row maps or decompressed table buffers.
Original values are stored as VARCHAR; numeric lookup keys and source-row
ordinals are separate internal columns. Numeric collisions in singleton tables
(e.g. TitleID `001` and `1`) fail loading.

`select` binds all distinct requested page identities into a request table and
executes one set-based [SQL query](src/bhl/metadata/join.sql). DuckDB performs
parent resolution, LEFT JOINs, relation membership, ordered list aggregation,
and missing/conflict detection. Each 1:N relation is aggregated independently
so authors, identifiers and subjects cannot multiply each other's rows.
Rust casts the nested Arrow result to the existing fixed 104-field metadata
schema and retains the selected rows for the page reader. The page reader then
fetches OCR bytes; it does not execute SQL or refetch TSVs.

Relations preserve original order and duplicate source rows. Creator identifiers
use membership across title and part creators, retaining each source row once
regardless of how often its creator occurs. DOI matches entity type as well as
numeric ID. Item and part rights stay in their separate original fields. Part
page sequence checks use matching partpage rows, not part.SequenceOrder.

Absent singleton rows become null children; absent relations become empty lists.
The outer metadata struct remains populated. Missing joins and conflicting
parents/sequences are returned in `BhlMetadataReport` and copied to schema
metadata. OCR records remain available; callers decide whether reconciliation
permits publication. Invalid headers/keys, missing tables, singleton collisions,
and source/decoder/database failures fail explicitly. Requests outside the
selected page set fail the provider. This does not normalize source values or
claim that unmatched upstream records have complete metadata.

## Resource and concurrency contract

A loaded value owns a private temporary directory containing a disk-backed
DuckDB database and its spill files. Both are deleted when it is dropped; there
is no persisted-index, resume or publication protocol. Reuse the loaded value
across selections/jobs to avoid downloading and ingesting all tables again.
A selection is self-contained and remains usable after dropping the database.

`BhlMetadata::load_with_options(storage, BhlMetadataOptions { ... })` accepts:

- `memory_limit`: DuckDB's buffer-manager limit, default `1GB`. This is not a
  process-memory cap. Ordered aggregates, Arrow conversion and selected output
  can allocate outside that limit, and queries can still run out of memory.
- `temporary_directory`: optional parent for the private database/spill directory.
  Provision disk for decoded data and query intermediates, not just gzip bytes.

Batch insertion is dispatched to Tokio's blocking pool. `select` is synchronous
and serializes queries on the loaded connection; async applications should call
it from a blocking worker for large selections. All selected Arrow rows remain
in memory, so callers should use bounded selections. No full-corpus memory,
throughput or spilling benchmark is claimed.

## Provenance

`BhlMetadataInfo` records canonical AWS paths, source row counts and SHA-256
fingerprints of decoded lexical cell rows, plus a combined fingerprint. They
are not compressed-object hashes or AWS version IDs. The storage may be a
caller-pinned mirror, while reported paths describe the canonical AWS objects;
retain the mirror/snapshot identity separately.

Acquisition spans a time window and does not guarantee an atomic AWS export.
Preserve source tables and their provenance when reproducibility requires an
immutable snapshot. `schema_metadata()` attaches fingerprint, source counts and
reconciliation to emitted page batches.
