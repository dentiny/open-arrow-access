# Source columns and positioned decoding

The decoder owns record boundaries and coordinates. It returns decoded Arrow
batches paired with row positions; the source-column adapter turns those
positions into named columns. Records are parsed once and never reserialized to
be passed to another reader.

```rust,ignore
use open_arrow_access::{FactoryAccessor, file_format::warc::WarcArrowFormat};
use open_arrow_access::layers::{SourceColumns, SourcePositionLayer};

// Raw storage -> frame-aware bytes -> positioned Arrow batches -> named columns.
let bytes = storage.layer(SourcePositionLayer::default());
let decoded = FactoryAccessor::new(bytes, WarcArrowFormat.with_source_positions());
let records = SourceColumns::new()
    .with_object("source_path")
    .with_offset("record_offset")
    .with_offset_in_frame("record_offset_in_frame")
    .wrap(decoded);
```

Enable `format-warc` in the facade (file-format crate: `warc`). Enable
`source-positions` for the optional frame-aware byte adapter. The positioned WARC
default format uses the usual `ReadRequest` and `ReadOptions.batch_size`, plus a
soft 16 MiB body-byte batch budget. Dates are appended directly as millisecond
timestamps without formatting and reparsing strings. Both ordinary and positioned
WARC readers expose unchanged body bytes as `BinaryView` by default: long bodies
retain the parser allocation, while values up to 12 bytes are stored inline.
Projection and source coordinates are unchanged. One oversized record is emitted
alone; there is no separate body-view option or decoder type.
warc-rs owns header parsing,
its header-length bound, required-field validation, body lengths and terminators.
Inter-record whitespace is skipped before marking the first record byte.
Malformed records return `Decode`; storage failures retain their categories.

`SourceColumns` is an explicit adapter: it consumes either an ordinary
`RecordReader` or a `PositionedRecordReader` and returns an ordinary Arrow reader.
It changes the reader type, so it is not a type-preserving `Layer`. Object-only
columns work with ordinary Arrow accessors using `ReadRequest`. Offsets on an ordinary reader
fail with `Unsupported` before opening the source; choose a positioned decoder
instead. No caller-supplied framer is required.

## Decoder contract

`PositionedBatch::try_new(batch, positions)` requires exactly one `BytePosition`
per row, including for zero-column projections. Coordinates travel in the same
value as their rows. `PositionedRecordReader` has the normal `Reader` lifecycle,
fixed Arrow schema in `RecordMetadata`, and terminal errors/EOF. The source-column
adapter checks schema consistency on every batch before exposing columns. It
streams batches without buffering a whole input or synchronizing side channels.

Custom formats implement the existing `ReaderFactory` protocol with
`type Reader = dyn PositionedRecordReader`. This reuses preparation before byte
storage opens, projection, object context and owned reader lifecycle. External
indexes can also construct positioned batches directly, without byte framing or
WARC-specific hooks.

`map_positioned_reader` supports row-preserving transforms and validates that the
row count remains aligned. `SchemaEvolutionLayer::wrap_positioned_reader` compiles
one evolution plan and carries coordinates through casts and generated columns.
Filtering or reordering rows must update coordinates together with the rows.

Names must be unique and must not collide with data fields. Object columns are
Utf8; offsets are UInt64. Projection resolves against the final selected names;
an empty projection retains row counts and still validates records. Schema
metadata is retained. Generated expressions can use source columns, then remove
them. Empty sources retain the complete output schema.

## Coordinates and composition

| Input | `record_offset` | `record_offset_in_frame` |
| --- | --- | --- |
| Uncompressed | first stored byte of the record | 0 |
| One gzip member per record | stored member start | 0 |
| Whole-file frame | 0 | decoded record offset |
| Several records per frame | stored frame start | decoded offset within the frame |

Records may cross compression members. Resume by decoding from the stored
offset and skipping the in-frame decoded offset before parsing the record.
Coordinates refer to one stable object version.

`SourcePositionLayer` takes raw storage bytes, recognizes `.gz`, `.tgz`, `.bz2`,
`.zst`, `.zstd` and `.lz4`, and preserves frame origins across decoding. Install it
before the positioned format instead of automatic decompression. Already-decoded
input cannot recover stored coordinates and is rejected. Plain positioned storage
needs no compression adapter. Object-only columns can use existing decompression.

Wrap the decoded accessor before FileSet, schema evolution and parallel reading.
The source-column adapter exposes a streaming whole-input scan; FileSet can run
independent files in parallel. It does not invent a separate scan partition for
each WARC record or reopen the format reader for each record. Memory use follows
the decoder batch size, record sizes and the parallel file budget.

## Migration

Replace `SourceColumnsLayer::with_framer` and `SourceUnit.bytes` with a positioned
decoder and `SourceColumns::wrap`. `SourceFramer`, `SourceUnit`, and `WarcFramer`
are removed. Ordinary object-only users replace `.layer(columns)` with
`columns.wrap(accessor)`. Custom byte framers should move record parsing into
their format and emit `PositionedBatch` values directly.
