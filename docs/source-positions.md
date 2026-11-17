# Source position columns

File readers can opt into provenance with `ReadOptions::source_columns`:

| Column | Reserved name | Arrow type |
| --- | --- | --- |
| `SourceColumn::Object` | `_source_object` | non-null Utf8 |
| `SourceColumn::Offset` | `_source_offset` | non-null UInt64 |
| `SourceColumn::OffsetInFrame` | `_source_offset_in_frame` | non-null UInt64 |

Columns append after projected data fields in request order. Duplicate requests
and conflicting names are `InvalidInput`. All Arrow file formats support the
object locator; WARC supports both position columns. Other formats reject offset
requests before opening bytes. JSONL/ATIF positions are a later extension.

For application column names, use:

```rust,ignore
SourceColumnsLayer::new()
    .with_object("source_path")
    .with_offset("record_offset")
    .with_offset_in_frame("record_offset_in_frame")
```

The layer also forwards scan plans and renames every partition. Recommended order:
`FileSet → ParallelReadLayer → SourceColumnsLayer → SchemaEvolutionLayer → SamplingLayer`.
Generated SQL columns can reference these fields and remove them afterwards.

## Coordinates

`BytePosition::Stored(n)` identifies byte `n` in an uncompressed stored object.
`BytePosition::Frame { frame, decoded }` identifies the compressed frame's stored
starting byte and an offset into that frame's decoded bytes. Record positions
refer to the first byte of the WARC header, including for records spanning frames.

| Input | Offset | Offset in frame |
| --- | --- | --- |
| Uncompressed | record's stored byte offset | 0 |
| One gzip member per record | member's stored byte offset | 0 |
| One frame for the whole object | 0 | record's decoded byte offset |
| Several records per frame | frame's stored byte offset | record's decoded byte offset in frame |

The pair identifies a record within an object with stable content. To resume,
start decoding at `offset`, skip `offset_in_frame` decoded bytes, then parse.
For Common Crawl record-at-a-time gzip, `offset` is the CDX/WARC member offset.
Store object versions or fingerprints separately in the pipeline manifest.

## Byte adapters and codecs

`ByteReader::position()` describes the next returned chunk. The default `None`
is deliberate: byte transformations must provide correct coordinates or leave
positions unknown. Storage exposes `Stored(cursor)`; range reads do not change
that cursor. Byte metrics wrappers forward the position.

`into_positioned_async_read` retains each chunk's origin and advances only on
`consume`, excluding buffered read-ahead. Its observer can mark the first byte
consumed by the next parser operation, resolving exact frame-boundary starts.

`ByteReadOptions::track_positions` defaults to false. With tracking disabled,
compression uses the existing multi-member path. With tracking enabled, gzip,
bzip2, zstd and lz4 decode one frame at a time. Output chunks never cross frame
boundaries. A one-chunk look-ahead resolves the next chunk's origin before
`position()` is queried; errors encountered there are delivered on the next pull.
Empty, corrupt, truncated and trailing compressed data remain decoding errors.

Custom codecs implement `Codec::frame_decoder` and `FrameDecoder::into_input` to
return buffered compressed input without consuming the following frame. Its
default rejects tracking with `Unsupported`. Unknown upstream byte coordinates
stay unknown; requesting WARC positions then fails with `Unsupported` and object
context rather than emitting fabricated offsets. Nested compression cannot map
the inner compressed frames to stored-object bytes and also stays unknown.

External file requests implement `FileRequest` to describe their output and
provenance wrapping; standard Arrow factories using `ReadRequest` work unchanged.
Factories declare supported columns with `ReaderFactory::supports_source_column`.
