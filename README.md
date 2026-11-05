# open-arrow-access

Composable dataset access for Apache Arrow. Read diverse formats and storage
backends into Arrow streams.

The initial framework supports **one local Parquet file**, read asynchronously
through **OpenDAL's filesystem service**. Output is an ordered stream of Arrow
record batches. The project ends at that read interface.

## Quick start

Enable the two independent features, `storage-fs` and `format-parquet`:

```sh
cargo run --example read_parquet --features storage-fs,format-parquet -- /absolute/data/root part.parquet
```

```rust,no_run
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    file_format::ParquetAccessor,
    storage::FsAccessor,
};

async fn read() -> Result<()> {
    let files = FsAccessor::new("/absolute/data/root").await?;
    let parquet = ParquetAccessor::new(files);
    let mut reader = parquet.open(ReadRequest::new("part.parquet")).await?;
    let schema = reader.metadata().schema.clone();

    while let Some(batch) = reader.next().await? {
        assert_eq!(batch.schema(), schema);
        // Consume the batch here.
    }
    Ok(())
}
```

`into_stream(reader)` adapts the reader to a standard
`Stream<Item = Result<RecordBatch>>`. `ReadOptions` supports a nonzero target
batch size and top-level field projection in the requested field order.

Storage I/O settings are per open operation:

```rust
use std::num::NonZeroUsize;
use open_arrow_access::{ByteReadOptions, ReadRequest};

let request = ReadRequest::new("part.parquet").with_byte_options(ByteReadOptions {
    io_concurrency: NonZeroUsize::new(4).unwrap(),
    io_chunk_size: NonZeroUsize::new(4 * 1024 * 1024),
});
```

The default I/O concurrency is one; the default I/O splitting size is backend
defined. These settings preserve byte/record ordering and do not enable the
future file/partition `ParallelReadLayer`. Each opened reader has its own limit.
`FsAccessor::with_chunk_size` controls sequential output chunk size, while
`ReadOptions::batch_size` controls Arrow rows per batch.

## Initial scope

| Component | Implemented |
| --- | --- |
| `core/` | Shared `Reader`, `Accessor`, `Layer`, `Capability`, byte/record specializations, errors, options |
| `storage/` | OpenDAL Fs, bounded sequential chunks and exact range reads |
| `file-format/` | Parquet schema inspection and streaming Arrow decoding |
| `dataset/` | Reserved; no directory or multi-file scan yet |
| `service/` | Reserved; no Hugging Face or other source adapter yet |
| `decompression/` | Reserved; no external decompression or archive support yet |
| `layers/` | Reserved; only the public layer contract and test-only wrappers exist |

Default features are empty. Only implemented capabilities have Cargo feature
names. Parquet page compression currently supports uncompressed and Snappy
pages through the Parquet dependency; this does not enable external `.gz`
decompression. Parallel reads and unordered emission are not implemented.

## Extension contracts

```text
Accessor::open(request).await -> Reader
Accessor::capability() -> effective operation support
Reader::capability()   -> opened representation's operation support
Reader::metadata()      -> stable typed metadata
Reader::next().await    -> next Bytes or RecordBatch
Layer::layer(accessor)  -> accessor with the same request/reader contract

OpenDAL Fs -> ByteReader -> ParquetAccessor -> RecordReader -> Arrow stream
```

The same generic accessor/layer protocol applies to bytes and records. Their
associated types keep compressed bytes distinct from decoded Arrow batches.
I/O methods (`open`, `list`, `next`, `read_range`, `read_ranges`) use `async fn`; implementations
apply the re-exported `#[async_trait]` attribute to preserve `dyn` compatibility
and Send futures. Capability, metadata and configuration-only methods remain
synchronous. Fs initialization is also asynchronous:
`FsAccessor::new(root).await?` moves OpenDAL's synchronous builder to Tokio's
blocking pool. Call it within a Tokio runtime.

`capability()` follows OpenDAL's reporting pattern, with `read`,
`read_with_range`, and accessor-only `list` flags. Layers forward or update
them to match the operations they actually expose. `ByteReader::read_range`
is optional; sequential-only inputs do not have to emulate seeking. The initial
Parquet adapter requires range reads and a known length. Runtime capabilities
describe a composed instance; Cargo features make implementations available.

Byte accessors accept `ByteReadRequest`, which carries an `ObjectRef` and
`ByteReadOptions`. Existing string locators can still use `open("path".into())`.
`read_ranges` preserves request order and has a sequential fallback for custom
readers; the OpenDAL backend uses batched fetches. Byte layers can delegate the
bulk method or use its fallback to intercept each individual range.
Small ranges are fetched in batches totaling at most 2 MiB, with adjacent and
overlapping ranges coalesced within each batch; unrequested gaps are not read.
Larger ranges stream into their final contiguous allocation. The adapter uses
2 MiB I/O chunks unless overridden, avoiding a full fragmented copy of every
requested range alongside its contiguous output. Returned bytes and decoder
memory are still proportional to the requested data; see the
[memory benchmark](docs/performance.md) for measurements and limits.

Third-party crates can implement these public contracts directly. A format
does not receive an OpenDAL operator or switch on storage schemes. Shared
OpenDAL reading code is separate from the filesystem constructor so the S3
adapter can reuse it. See [architecture](docs/architecture.md) for the contracts
and [roadmap](docs/roadmap.md) for the independent follow-up PRs.

`ReadOptionsProvider` lets record layers inspect or adjust projection settings
on file, dataset or service request types. Generated-column evaluation is a
required follow-up feature; its [design](docs/generated-columns.md) specifies
expression dependencies and augmented output schema without adding an expression
engine to the initial framework.

## Runtime behavior

- Read and emit in input order. Storage I/O concurrency is opt-in and defaults
  to one. There is no library-managed file/partition concurrency.
- Schema, including schema metadata, is available before the first batch and
  matches every returned batch. Batch boundaries may differ from row groups.
- Errors and EOF terminate record reading. Input error categories and source
  chains survive the Parquet bridge; transparent replay is not performed.
- Drop the reader to stop work. After cancelling an in-flight decoder call,
  drop that reader rather than assuming it can resume.
- Filesystem objects must remain unchanged during a read. This version does
  not provide object snapshots. Object paths are root-relative; the configured
  root follows normal host symlink semantics.
- The byte chunk size and Arrow row target bound those units, not total decoder
  memory or the size of an individual value/Parquet row group.

## Development

Dependency versions track a recorded latest upstream Lance commit, including
resolved patch versions. See [dependency alignment](docs/dependencies.md) for
the baseline and update procedure. DataFusion is pinned for the future layer
but is not part of the current dependency graph.

```sh
cargo fmt --all -- --check
cargo check --workspace --no-default-features
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Tests generate their own small Parquet fixtures. They verify ordered rows and
nulls across row groups, schema metadata, projection, storage-independent
reading, wrapper composition, exact/batched ranges, per-open I/O options,
bounded backend concurrency, and terminal error/drop behavior.
Fixture generation is test-only; the library exposes no writer or sink API.
