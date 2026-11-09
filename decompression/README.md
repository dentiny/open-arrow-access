# Streaming decompression

Features are default-off. Apply a codec explicitly; file extensions do not
select a decoder. Use streaming codecs for a decoded byte stream, or the
[ZIP](#zip-entries) and [TAR](#tar-entries) APIs to select archive members.

| Codec | Facade feature | Crate feature | Marker |
| --- | --- | --- | --- |
| Gzip | `decompression-gzip` | `gzip` | `Gzip` |
| Bzip2 | `decompression-bz2` | `bz2` | `Bzip2` |
| LZ4 | `decompression-lz4` | `lz4` | `Lz4` |
| Zstandard | `decompression-zstd` | `zstd` | `Zstd` |

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, ByteReadRequest,
    decompression::{DecompressionLayer, gzip::Gzip}, storage::{OpenDalAccessor, opendal},
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

## ZIP entries

Enable `decompression-zip` on the facade or `zip` on this crate for
`ZipAccessor`. ZIP contains named entries, so select a central-directory index
instead of concatenating files through `DecompressionLayer`.

```rust,no_run
use open_arrow_access::{
    Accessor, ByteReadRequest, decompression::zip::ZipAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read(operator: opendal::Operator) -> open_arrow_access::Result<()> {
    let zip = ZipAccessor::new(OpenDalAccessor::new(operator), 0);
    let request = ByteReadRequest::new("paper.meca.zip");
    for entry in zip.entries(request.clone()).await? {
        println!("{}: {:?}", entry.index, entry.name);
    }
    let mut reader = zip.open(request).await?;
    while let Some(bytes) = reader.next().await? {
        // Original decompressed bytes of entry 0, including binary PDF content.
    }
    Ok(())
}
```

`entries` returns the central directory in order, retaining raw filename bytes,
duplicate names, directories, and compressed/uncompressed lengths. Select by
index to distinguish duplicate names. Names are data; no filesystem extraction,
path normalization, symlink following, or recursive archive expansion occurs.
The archive's storage identity must remain stable between inspection and open.

Stored and Deflate entries, ZIP64, and data descriptors are supported through
the published `async_zip` parser. Encrypted entries, split archives, and other
compression methods are unsupported. Input requires exact ranges and a known
length, including for Stored entries. Sequential-only sources and an outer
gzip layer need explicit materialization before this accessor can read them.

The directory is read at inspection/open time; entry decompression is pulled
on demand. Each compressed range fetch is at most 64 KiB; decoded output chunks
are at most 64 KiB by default (`with_chunk_size` changes the latter). Only ZIP
metadata, decoder state and read-ahead are retained, without collecting the
archive or whole entry. No background tasks are started.

`ZipLimits` defaults to 1 GiB each for compressed/decoded entry size.
`with_limits` overrides that value. The parser also checks actual decoded length against the limit.
CRC and declared decoded length are validated when the entry reaches EOF;
earlier chunks may already have been emitted. Read through EOF for validation.
Errors and EOF are terminal and upstream error categories are preserved.

The output implements `ByteReader` and `ZipAccessor` implements `ByteAccessor`,
so a selected entry composes with `JsonAccessor`, `XmlAccessor`, or other
sequential consumers. Output ranges and prefix listing are unsupported.
`ByteMetadata.content_length` is the declared decoded entry size; source and
version identify the archive. Storage requests and byte options pass through
unchanged. The selected index belongs to the accessor, never to a synthetic
storage path.

Run the local example to inspect a ZIP and read one entry:

```sh
cargo run --example read_zip --features decompression-zip -- testdata/zip mixed.zip 1
```

## Decoder output adapter

`decoded_reader(decoder, metadata, chunk_size, operation, codec_name)` wraps any
owned futures AsyncRead decoder in the standard sequential ByteReader contract.
It shares chunking, source-error preservation and terminal handling between
stream codecs and selected archive entries. Callers supply decoded-coordinate
metadata and codec-specific configuration; the helper performs no eager I/O.

## Reusing a ZIP directory

`ZipAccessor::archive(request).await` opens one `ZipArchive` session. Its
`entries()` borrows the cached directory, and `open_entry(index, chunk_size)`
returns an independently owned reader over the same opened source. Cloning a
session shares metadata and the source without reparsing the directory. Readers
can be interleaved and outlive the session. The direct entry futures are owned
and Send; existing source-version guarantees are retained.

For an already opened ByteReader, use `ZipArchive::open(input, limits).await`.
Existing `ZipAccessor::entries` and `open` convenience methods remain available;
use a session when reading multiple entries to avoid repeated storage opens and
directory parsing. The `read_zip` example demonstrates this session path.

## TAR entries

Enable `decompression-tar` for two access patterns over regular GNU/USTAR files
and directories. Links, sparse files, and extension headers are unsupported.
Entry names remain raw metadata; neither API extracts paths to the filesystem.

| API | Input | Use when |
| --- | --- | --- |
| `TarStream::new(input, limits)` | Any sequential `ByteReader` | Reading an archive once in physical entry order, including after gzip for `.tar.gz` |
| `TarArchive::open(input, limits).await` | Exact ranges and known length | Inspecting headers once and opening selected entries independently |

`TarStream::next_entry().await` returns a header and lazy body reader without
building an upfront index. Consume or drop the active body before advancing;
unread bytes from a dropped body are drained by the next call. If a body read is
cancelled, drop that body reader. Header/skip progress is retained across
cancellation of archive advancement.

`TarArchive::entries()` borrows cached headers; `open_entry(index, chunk_size)`
returns an owned sequential byte reader. Entry readers have independent cursors
and can outlive the archive. Header inspection uses range requests rather than
collecting entry bodies. The current implementation issues one header request
per entry, so use `TarStream` for a sequential bulk scan.

`TarLimits` defaults to 65,536 entries and 1 GiB per entry. Entry metadata retains
the archive's source/version and declares the selected entry's byte length.
Selected entry readers are sequential; they do not provide Parquet-compatible
range access. No checksum metadata supplied by a provider is automatically
verified by TAR reading.

Run the [TAR example](../examples/read_tar.rs) against an uncompressed archive:

```sh
cargo run --example read_tar --features decompression-tar -- /absolute/data/root archive.tar
```

For a compressed archive, open it through `DecompressionLayer::new(Gzip)` and
pass the decoded byte reader to `TarStream`. Ranged `TarArchive` cannot consume
that sequential decoded representation directly.
