# Streaming decompression

Features are default-off. Apply a codec explicitly; file extensions do not
select a decoder.

| Codec | Facade feature | Crate feature | Marker |
| --- | --- | --- | --- |
| Gzip | `decompression-gzip` | `gzip` | `Gzip` |
| Bzip2 | `decompression-bz2` | `bz2` | `Bzip2` |
| LZ4 | `decompression-lz4` | `lz4` | `Lz4` |
| Zstandard | `decompression-zstd` | `zstd` | `Zstd` |

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, ByteReadRequest,
    decompression::{DecompressionLayer, Gzip}, storage::{OpenDalAccessor, opendal},
};

async fn read() -> Result<(), Box<dyn std::error::Error>> {
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root("/absolute/data/root"),
    )?;
    let files = OpenDalAccessor::new(operator);
    let gzip = files.layer(DecompressionLayer::new(Gzip));
    let mut reader = gzip.open(ByteReadRequest::new("records.jsonl.gz")).await?;
    while let Some(bytes) = reader.next().await? {
        // Pass decoded bytes to a sequential consumer.
    }
    Ok(())
}
```

OpenDAL storage is always available; enable the upstream `services-fs` backend
for this example. The layer implements the existing
`Layer`, `Accessor` and `ByteReader` protocols, so custom storage and nested
compression wrappers work without format-specific code. Requests and storage
I/O options pass through unchanged. Discovery delegates to the underlying byte
accessor and retains its locator semantics; discovery does not filter inputs.

Gzip uses `async-compression`'s futures decoder with multi-member decoding,
matching the decoder configuration in ml-infra's
`lance/lance_migration/crates/inspector/src/compression.rs`. Concatenated members,
including empty members, decode through EOF. Truncated headers/trailers, invalid
checksums and non-gzip trailing bytes are errors. A valid empty gzip member is
EOF; an empty compressed object is invalid. Validation finishes only when the
consumer reads through EOF, so a later member/checksum can fail after earlier
decoded bytes were delivered.

Decoded `content_length` is unknown. The source locator and version still
identify the original compressed object; the version is not a decoded-content
hash. Accessor and reader range capabilities are cleared. Exact range methods
return Unsupported without moving the sequential cursor. Listing remains an
accessor-only capability. External `.parquet.gz` therefore needs a future
explicit indexing/materialization strategy before the Parquet range reader can
use it; this layer provides neither.

Decoding is pull-based, with no background tasks or whole-input collection.
Decoded output chunks are at most 64 KiB by default; `with_chunk_size` changes
that limit independently of compressed I/O chunking. Codec state, upstream
chunks and consumer-held outputs are additional memory. Upstream errors retain
their category and original source; codec corruption uses Decode. Errors and
EOF are terminal. Drop a decoder after cancelling an in-flight read.

`DecompressionFactory<C>` implements core `ReaderFactory` for byte requests and
byte output. Both decompression and file accessors use the blanket
`ReaderFactoryExt::open_reader` implementation to prepare configuration before
opening storage, then create the output reader.
The factory uses `read_chunks` for decoded output and `StreamReader` for metadata,
sequential capabilities and terminal handling. Codec error mapping stays here;
listing remains delegated by `DecompressionAccessor`.

To add a codec, implement `Codec` using an upstream streaming decoder in its
own module and enable its dependency feature. `DecompressionLayer` and its
accessor/factory adapters remain shared. The `CompressedInput`/`DecodedInput`
aliases expose the futures I/O boundary without coupling a codec to storage,
Arrow or a closed core compression enumeration.

## Bzip2 members

Bzip2 uses `async-compression`'s `BzDecoder` with multi-member decoding, matching
ml-infra's existing compression adapter. Concatenated members, including empty
members, are decoded in order. Invalid headers, truncated members, checksum
failures and non-bzip2 trailing bytes produce terminal decode errors. Empty
compressed input is invalid; a valid empty member yields EOF. As with gzip,
checksum validation completes only when the consumer reads through EOF.

Use `DecompressionLayer::new(Bzip2)` with the same byte-layer API. Bzip2 codec
state includes a compression block, so bounded output chunks do not bound total
memory or guarantee that compressed input consumption stops within one output
chunk. The decoder is pull-based and does not collect whole objects or create
background tasks.

## LZ4 frames

`Lz4` uses `async-compression`'s futures decoder backed by liblz4. It supports
standard LZ4 frames, concatenated frames, and skippable frames. Raw LZ4 blocks
and legacy frames are rejected; there is no external-dictionary configuration.
Checksums are validated when present. Truncated frames, corrupt checksums and
non-frame trailing bytes are errors. An empty or skippable frame produces no
bytes; an empty compressed object is invalid. Read through EOF to validate all
frames. The shared layer's metadata, error and memory contracts above apply.

Use `DecompressionLayer::new(Lz4)` in the example above. This follows the existing
ml-infra approach of adapting upstream asynchronous compression decoders, adding
LZ4 to the same generic wrapper rather than implementing another codec.

## Zstandard frames

For Zstandard, enable `decompression-zstd` and replace `Gzip` with `Zstd` in
the example. The adapter uses `async-compression`'s futures `ZstdDecoder` with
multi-frame decoding, as in ml-infra's compression helper. Standard frames,
concatenated frames, empty frames and skippable frames are supported. Invalid
checksums, truncated frames and trailing non-frame bytes are decode errors.
An empty frame or a skippable-only input is valid EOF; an empty compressed object
is invalid. Validation completes only when the consumer reads through EOF.
Raw blocks, legacy format and custom dictionary/decoder-parameter configuration
are outside this adapter. A stream requiring an unavailable dictionary fails
with a decode error. Codec state, including its frame window, is additional to
the emitted chunk limit.
