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
I/O methods (`open`, `list`, `next`, `read_range`) use `async fn`; implementations
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

- Read and emit in input order. There are no detached producer tasks or
  library-managed concurrent reads.
- Schema, including schema metadata, is available before the first batch and
  matches every returned batch. Batch boundaries may differ from row groups.
- Errors and EOF terminate record reading. Errors retain the original source
  chain; transparent replay of already delivered records is not performed.
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
reading, wrapper composition, exact ranges, and terminal error/drop behavior.
Fixture generation is test-only; the library exposes no writer or sink API.
