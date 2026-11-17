# SourceColumnsLayer

`SourceColumnsLayer` is an opt-in, format-independent layer over core's
`FactoryAccessor`. It uses an ordinary `ReaderFactory`; formats need no source
column hooks. The WARC implementation and existing decompression pipeline are
unchanged. The caller chooses the framer and the source column names.

```rust,ignore
let columns = SourceColumnsLayer::new()
    .with_framer(my_framer)
    .with_object("source_path")
    .with_offset("record_offset")
    .with_offset_in_frame("record_offset_in_frame");

// Optional byte adapter, enabled by the `source-positions` feature.
// It takes raw storage bytes, before any decompression.
let bytes = storage.layer(SourcePositionLayer::default());
let records = FactoryAccessor::new(bytes, WarcArrowFormat).layer(columns);
let source = FileSetAccessor::new(records)
    .with_union_by_name()
    .with_file_planning()
    .layer(ParallelReadLayer::new(concurrency, NonZeroUsize::MIN))
    .layer(SchemaEvolutionLayer::try_new(evolution)?);
```

Apply the layer before FileSet and parallel reading. Object-only columns do not
need a framer: omit `with_framer` and use the ordinary whole-input format reader.
Requesting offsets without a framer fails before opening storage. Installing no
source layer keeps the existing format reading path.

## User-provided framing

Implement `SourceFramer::frame(Box<dyn ByteReader>)` to return a stream of
`Result<SourceUnit>`. Each unit contains:

- `bytes`: complete input an ordinary format reader can decode independently,
  including any required headers.
- `positions`: one `BytePosition` per output row, in parser order.

The framer owns record boundaries and coordinate accuracy. A compression frame
is not automatically a record: one frame can contain several records, and a
record may span frames. The framer must account for records skipped or expanded
by the chosen parser. It can observe consumed byte positions using
`into_positioned_async_read`, or obtain coordinates from an external index.
The layer never inspects WARC headers or assumes one row per frame.

Units must cover the input once in logical order and produce the same schema.
Each unit becomes one lazy scan partition. Planning inspects the first unit;
later units are discovered as the execution window has room. Each partition
opens an ordinary format reader. Group several records per unit to amortize
reader creation. The layer buffers decoded batches within a unit and validates
its complete row count before exposing any batch, so memory usage depends on
unit size and the parallel partition budget. Inference must resolve the same
schema for each unit; use an explicit format schema when needed. An empty input
uses an empty ordinary format reader to establish its schema.

The WARC integration test supplies a caller-owned framer and groups two records
per unit. A separate CSV test supplies indexed coordinates through the same
interface, without compression or positioned input.

## Columns and coordinates

Object columns are Utf8; offsets are UInt64. Names must be unique and must not
collide with data fields. Projection resolves against the final chosen names,
including an empty selection that retains row counts. Generated SQL columns can
reference these names, then remove intermediate source columns.

| Input | `record_offset` | `record_offset_in_frame` |
| --- | --- | --- |
| Uncompressed | first stored byte of the record | 0 |
| One gzip member per record | stored member start | 0 |
| Whole-file frame | 0 | decoded record offset |
| Several records per frame | stored frame start | decoded record offset within the frame |

These semantics depend on the positions supplied by the framer. To resume,
decode from the stored offset, skip the in-frame decoded offset, then parse the
record. For Common Crawl, member starts match CDX coordinates. Coordinates refer
to one stable object version; keep versions or fingerprints in a pipeline
manifest.

## Optional byte adapter

`SourcePositionLayer` passes uncompressed bytes through and recognizes `.gz`,
`.tgz`, `.bz2`, `.zst`, `.zstd`, and `.lz4`. It decodes one compression frame at a
time and preserves unread stored bytes for the next frame. Output chunks never
cross frame boundaries; `with_chunk_size` controls their size. Install it on raw
storage in place of automatic decompression when framing needs stored offsets.
Installing it after a decoder fails because stored coordinates are unavailable.
For uncompressed positioned storage or externally indexed framing, this adapter
is unnecessary. Object-only columns can use existing decompression.

Core adds `BytePosition`, a default-unknown `ByteReader::position()`, and a
buffered observer that advances only on consume. OpenDAL exposes its stored
cursor and byte metrics forward it. Marking captures the first consumed byte,
including at chunk and compression-frame boundaries. Core factory/request APIs,
FileSet, all format implementations, and the decompression crate need no changes.
