# SourceColumnsLayer

`SourceColumnsLayer` adds an object locator and optional record positions. It owns
column selection, names and projection. Ordinary `ReadOptions`, `ByteReadOptions`,
file opens and the existing decompression crate are unchanged.

```rust,ignore
let columns = SourceColumnsLayer::new()
    .with_object("source_path")
    .with_offset("record_offset")
    .with_offset_in_frame("record_offset_in_frame");

// Enable the `source-positions` feature for the optional byte adapter.
// It takes RAW storage bytes, before any decompression.
let bytes = storage.layer(columns.byte_positions());
let format = WarcArrowAccessor::new(bytes);
let source = FileSetAccessor::new(format)
    .with_union_by_name()
    .with_file_planning()
    .layer(columns)
    .layer(ParallelReadLayer::new(concurrency, NonZeroUsize::MIN))
    .layer(SchemaEvolutionLayer::try_new(evolution)?);
```

Apply source columns before `ParallelReadLayer`. Its scan-plan extension renames
each partition and preserves file-internal planning. Generated SQL columns can
reference the chosen names, then remove the position columns. Projection may
select the chosen names, including an empty selection that retains row counts.

Object columns are Utf8; offsets are UInt64. Names must be unique and must not
collide with data fields. Every Arrow file format supports object locators.
WARC also supports record offsets; unsupported requests fail before opening
storage. JSONL/ATIF offsets are a later parser extension.

## Position semantics

| Input | `record_offset` | `record_offset_in_frame` |
| --- | --- | --- |
| Uncompressed | first stored byte of the record | 0 |
| One gzip member per record | stored member start | 0 |
| Whole-file frame | 0 | decoded record offset |
| Several records per frame | stored frame start | decoded record offset within the frame |

To resume, start decoding at the stored offset, skip the in-frame decoded offset,
then parse the record. Common Crawl member offsets match CDX coordinates. These
coordinates identify positions in one stable object version; keep object
versions or fingerprints in the pipeline manifest.

## Isolated byte adapter

`columns.byte_positions()` is an optional adapter implemented inside the layer.
It passes uncompressed bytes through and recognizes `.gz`, `.tgz`, `.bz2`, `.zst`,
`.zstd`, and `.lz4`. It consumes a single frame at a time and preserves unread
compressed bytes for the next frame. Output chunks never cross frame boundaries.
Its chunk size can be set independently with `with_chunk_size`.

This adapter replaces automatic decompression in the provenance-enabled branch.
It does not modify `Codec`, `CompressionFactory`, `DecompressionFactory`, or
`AutoDecompressionLayer`. Installing it after a decoder fails explicitly because
stored coordinates have already been lost. An ordinary decoder still returns
unknown positions, and a record-offset request on that input is Unsupported.

For uncompressed inputs, a storage reader that provides positions needs no byte
adapter. Object-only provenance can use the existing decompression pipeline.

## Small parser extension

Core adds `BytePosition`, a default-unknown `ByteReader::position()`, and buffered
position observation that advances only on consume. OpenDAL reports its stored
cursor; byte metrics forward it. The first byte consumed after a parser mark
identifies the record start even at a frame boundary.

`SourceColumnAccessor` / `SourceColumnPlanner` are opt-in extension interfaces.
FileSet forwards their selection with a configured accessor instead of adding
fields to every request. Default factory hooks support object columns and reject
offsets; WARC overrides those hooks to capture record starts in sequential and
planned reads. Existing factory and reader implementations retain default hooks.
