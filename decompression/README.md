# Streaming decompression

Each codec is an independent, default-off Cargo feature. Apply a codec
explicitly; file extensions do not select a decoder.

| Facade feature | Crate feature | Codec |
| --- | --- | --- |
| `decompression-gzip` | `gzip` | `Gzip` |
| `decompression-zstd` | `zstd` | `Zstd` |

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, ByteReadRequest, Result,
    decompression::{DecompressionLayer, Gzip}, storage::FsAccessor,
};

async fn read() -> Result<()> {
    let files = FsAccessor::new("/absolute/data/root").await?;
    let gzip = files.layer(DecompressionLayer::new(Gzip));
    let mut reader = gzip.open(ByteReadRequest::new("records.jsonl.gz")).await?;
    while let Some(bytes) = reader.next().await? {
        // Pass decoded bytes to a sequential consumer.
    }
    Ok(())
}
```

This example also needs `storage-fs`. The layer implements the existing
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

To add a codec, implement `Codec` using an upstream streaming decoder in its
own module and enable its dependency feature. `DecompressionLayer` and its
accessor/reader adapters remain shared. The `CompressedInput`/`DecodedInput`
aliases expose the futures I/O boundary without coupling a codec to storage,
Arrow or a closed core compression enumeration.
