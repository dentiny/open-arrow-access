# Streaming decompression

Enable a codec feature on the facade or this crate. All features are default-off.
Apply a codec explicitly; file extensions do not select a decoder.

| Facade feature | Crate feature | Codec |
| --- | --- | --- |
| `decompression-gzip` | `gzip` | `Gzip` |
| `decompression-lz4` | `lz4` | `Lz4` |

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
