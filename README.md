# open-arrow-access

Composable dataset access for Apache Arrow. Read diverse formats and storage
backends into Arrow streams.

Read local Parquet, CSV/TSV, JSON/JSONL, XML, or ATIF files asynchronously through **OpenDAL's filesystem
service**, individually or as a sequential directory/file-set dataset. Output is
an ordered stream of Arrow record batches. The project ends at that read interface.

## Quick start

Enable `format-parquet`; OpenDAL storage is always available. The example
enables OpenDAL's filesystem backend through its development dependencies:

```sh
cargo run --example read_parquet --features format-parquet -- /absolute/data/root part.parquet
```

```rust,no_run
use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::ParquetAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read() -> Result<(), Box<dyn std::error::Error>> {
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root("/absolute/data/root"),
    )?;
    let files = OpenDalAccessor::new(operator);
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

For small metadata objects, `read_limited(reader, limit, operation).await`
collects an owned `ByteReader` into a `Vec<u8>`, rejecting advertised or actual
lengths above the byte limit before copying. Parsing remains with the caller.

`read_lines(reader, max_line_bytes, operation)` streams raw byte lines, retaining
LF/CRLF endings and a final unterminated line. The optional per-line byte limit
includes line endings and is checked before copying. UTF-8 decoding and blank-line
handling remain with the caller.

`read_chunks(input, chunk_size)` adapts a futures `AsyncRead` into nonempty byte
chunks bounded by a `NonZeroUsize` size. Map its I/O errors to the shared error
type, then wrap it in `StreamReader::new(stream, byte_metadata)` for a sequential
`ByteReader`.

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
Sequential output reuses one OpenDAL stream; retained prefetch and range reads
share that reader's I/O budget. Output chunk size does not create extra storage
requests.
`OpenDalAccessor::with_chunk_size` controls sequential output chunk size, while
`ReadOptions::batch_size` controls Arrow rows per batch.

`OpenDalAccessor` is the only storage accessor. Enable backend features on your
own OpenDAL dependency; for the filesystem example above, use
`opendal = { version = "0.59.2", default-features = false, features = ["services-fs"] }`.

For other OpenDAL backends, depend on `open-arrow-access-storage` and the same
OpenDAL version directly. Enable upstream features on that dependency; Cargo
unifies them with storage's OpenDAL dependency. The generic `OpenDalAccessor`
is always available in the storage crate. For example, a downstream crate can
expose an `s3` feature:

```toml
[dependencies]
open-arrow-access-storage = "0.1.0"
opendal = { version = "0.59.2", default-features = false, features = ["http-transport-reqwest"] }

[features]
s3 = ["opendal/services-s3"]
```

Build that crate with `cargo build --features s3`, then wrap its configured
operator:

```rust
use open_arrow_access_storage::OpenDalAccessor;

let operator = opendal::Operator::new(
    opendal::services::S3::default().bucket("my-bucket").region("us-east-1"),
)?;
let accessor = OpenDalAccessor::new(operator);
```

The same approach supports OpenDAL's other service, layer and transport
features without adding matching features to this project. OpenDAL 0.59.2
names its S3 feature `services-s3`.

## JSON and JSONL

Enable `format-json` and construct `file_format::JsonAccessor::new(bytes, schema)`
with a fixed `SchemaRef`, then open it with the same `ReadRequest` used for
Parquet. `bytes` can be any sequential byte accessor; range reads and a known
length are unnecessary. Use `format-json` and enable OpenDAL's
`services-fs` backend on your OpenDAL dependency for local files.

The reader uses Arrow JSON's incremental decoder for whitespace-separated JSON
objects, including JSONL and pretty-printed object streams. Top-level arrays
are not flattened into rows; arrays, null and scalar records are rejected.
Empty or whitespace-only input emits no rows. Schema inference is not performed.
Missing nullable fields become null and unknown fields are ignored; projected
fields are decoded in the requested order. An empty projection retains row counts.
The supplied schema, including metadata, is fixed before reading any bytes.

Rows are decoded incrementally up to the requested batch size. Parsing follows
Arrow JSON 59.3.0's permissive semantics; this adapter does not
add a strict JSON validator. Errors reported by the decoder, including truncated
records, terminate the reader with a decode error. Input I/O failures retain
their original category. Memory includes one upstream chunk and the decoder's
current batch; batch size does not bound the size of a single record.

## CSV and TSV

Enable `format-csv` for CSV/TSV with an explicit schema. `CsvAccessor` consumes
sequential byte readers and reuses Arrow CSV's incremental decoder. Configure
`CsvFormat::default().with_header(true)` for headers or `.with_delimiter(b'\t')`
for TSV.

## Supported components

| Component | Implemented |
| --- | --- |
| `core/` | Shared `Reader`, `Accessor`, `Layer`, `Capability`, byte/record specializations, errors, options |
| `storage/` | One OpenDAL accessor over caller-configured backends, recursive discovery, bounded chunks and exact ranges |
| `file-format/` | Parquet schema inspection; CSV/TSV and JSON/JSONL with explicit schemas; XML with inferred or explicit schemas; ATIF with a fixed trajectory schema; WARC parser records with Arrow conversion deferred |
| `dataset/` | Default-off directory and explicit file-set composition over supplied record accessors |
| `service/` | Shared provider crate scaffold; provider implementations arrive under separate default-off features |
| `decompression/` | Streaming gzip, LZ4, bzip2, and Zstandard byte layers with concatenated-frame/member support |
| `test-support/` | Unpublished, dev-dependency-only byte backend shared by format and decompression tests |
| `layers/` | Reserved; only the public layer contract and test-only wrappers exist |

Default features are empty. Only implemented capabilities have Cargo feature
names. Parquet page compression currently supports uncompressed and Snappy
pages through the Parquet dependency. External gzip, LZ4, bzip2, and Zstandard decoding are separately
enabled by `decompression-gzip`, `decompression-lz4`, `decompression-bz2`, and `decompression-zstd`; see [streaming decompression](decompression/README.md).
It produces sequential bytes and cannot directly satisfy Parquet's range reads.
Parallel reads and unordered emission are not implemented.

Enable `dataset-directory` for `DirectoryAccessor` and `FileSetAccessor`. Directory
selection uses a supplied byte accessor for discovery and a supplied format
accessor for records. Files are sorted lexicographically; explicit file sets
retain caller order. Readers open files sequentially and require identical
projected schemas, including metadata.

Discovery accepts `ListRequest`, with recursion enabled by default. Setting
`DirectoryRequest::recursive` to `false` prevents descending into child directories.
Repeated and trailing separators in directory prefixes are normalized before
discovery and result matching.

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
synchronous. Construct the OpenDAL operator before wrapping it with
`OpenDalAccessor::new(operator)`. Backend initialization and configuration belong
to the caller; a synchronous backend builder can run on a blocking pool when
configuration happens inside an async task.

`capability()` follows OpenDAL's reporting pattern, with `read`,
`read_with_range`, `read_parallel`, and accessor-only `list` flags. Layers forward or update
them to match the operations they actually expose. `ByteReader::read_range`
is optional; sequential-only inputs do not have to emulate seeking. The initial
Parquet adapter requires range reads and a known length. Runtime capabilities
describe a composed instance; Cargo features make implementations available.

`read_parallel` declares support for reading multiple files or partitions
concurrently within an interface, independently of range access and output
ordering. Existing concrete implementations report `false`; transparent
wrappers forward the flag. This declaration does not schedule work or change
per-reader byte I/O concurrency.

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
memory are still proportional to the requested data.

Third-party crates can implement these public contracts directly. A format
does not receive an OpenDAL operator or switch on storage schemes. One
`OpenDalAccessor` wraps caller-configured filesystem, S3, and other OpenDAL
operators. See [architecture](docs/architecture.md) for the contracts
and [roadmap](docs/roadmap.md) for the independent follow-up PRs.

`ReadOptionsProvider` lets record layers inspect or adjust projection settings
on file, dataset or service request types. Generated-column evaluation is a
required follow-up feature; its [design](docs/generated-columns.md) specifies
expression dependencies and augmented output schema without adding an expression
engine to the initial framework.

The [source-attribute design](docs/source-attributes.md) describes future
partition planning and ordering constraints across formats, datasets and provider
services, including globally ordered ATIF inputs and conditional WARC indexing.

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
the baseline and update procedure. DataFusion is declared for the future layer
but is not part of the current dependency graph.

```sh
cargo fmt --all -- --check
cargo check --workspace --no-default-features
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Start at the [test index and placement rules](tests/README.md) to find or add a
suite. Each crate owns tests in its `tests/` directory; the root `tests/` covers
facade wiring and cross-crate composition. CSV/TSV, JSON/JSONL, XML and ATIF suites
live together in `file-format/tests/`. Private implementation tests, when needed,
belong in the owning crate's `tests/unit/`, explicitly included by its library.
Optional test targets declare `required-features` in `Cargo.toml`.

Tests read checked-in Parquet, CSV/TSV, and JSON/JSONL files from `testdata/`. They verify ordered rows and
nulls across row groups, schema metadata, projection, storage-independent
reading, wrapper composition, exact/batched ranges, per-open I/O options,
bounded backend concurrency, and terminal error/drop behavior.
Tests may copy fixtures into temporary roots to exercise storage behavior.
The library exposes no writer or sink API.

## XML

Enable `format-xml` and supply the input accessor and absolute record element path.
The output schema is inferred automatically:

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use open_arrow_access::{
    Accessor, ReadRequest, file_format::XmlAccessor,
    storage::{OpenDalAccessor, opendal},
};

let root = std::env::current_dir()?.join("testdata/xml");
let operator = opendal::Operator::new(
    opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
)?;
let xml = XmlAccessor::new(OpenDalAccessor::new(operator), "/inventory/warehouse");
let mut reader = xml.open(ReadRequest::new("inventory.xml")).await?;
while let Some(batch) = reader.next().await? { /* consume batch */ }
# Ok(())
# }
```

Each `open` samples at most 128 records and 8 MiB of serialized XML by default,
fixes the schema, then replays sampled records before continuing the same stream.
Configure these bounds with `with_inference_options(XmlInferenceOptions { .. })`.
Opening an inferred reader performs input I/O; sampling errors are returned by
`open`. Attributes become `@name` columns, repeated elements become lists, and
nested elements become structs. Fields absent from the sample are not added
later; later values incompatible with inferred types produce a terminal error.
Schemas are inferred independently for each opened file.

Explicit mappings remain available through `.with_schema(schema)` when column
names or types must be fixed by the caller. For inference with namespace-prefixed
record paths, use `.with_namespaces(&[NamespaceBinding { prefix, uri }])`;
explicit schemas carry their own namespace bindings. Namespace URIs in inferred
fields use the supplied prefixes, with generated prefixes for other namespaces.

`XmlField::structure`, `XmlField::list`, and URI-based `NamespaceBinding` mappings
support nested records and namespaces. Paths select direct children, terminal
attributes (`@id`), or direct scalar text (`.`); they are not XPath. Unprefixed
paths select elements with no namespace. Types are Utf8, Boolean, Int64, finite
Float64, Struct and List. Missing nullable elements and `xsi:nil` map to null;
empty strings and empty lists remain distinct. Mixed text and child elements
cannot be mapped to scalar/struct/list fields as rich text.

The shared `ReadRequest` controls row targets, projection in caller order
(including zero columns), and byte I/O options. The schema is fixed across batches.
`XmlLimits` defaults to 8 MiB per record/event, 64 MiB of serialized records per
batch, and depth 128. These are XML limits, not a bound on total Arrow memory;
one lookahead record and an upstream byte chunk may also remain buffered.
Skipped content is only tokenized by quick-xml and remains subject to event/depth
limits; attribute/entity errors outside selected records may go unreported. Inputs
must be UTF-8; DTDs and custom entities are rejected. Errors are terminal.

External compression remains independent: compose `XmlAccessor` over an accessor
with `DecompressionLayer::new(Gzip)` for `.xml.gz`. FileSet and directory reading
reuse the same format accessor and preserve file/record order. No format
auto-detection, index or parallel scheduler is added by this adapter.
With explicit mappings, projection avoids conversion of omitted fields. In
inference mode, projection names are resolved after sampling and output columns
are selected after conversion. Selected records are read and parsed in full in
both modes. Run the example on the project-authored inventory fixture with
`cargo run --features format-xml --example read_xml`.

## ATIF trajectories

Enable `format-atif` and select the document layout explicitly:

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::{AtifAccessor, AtifFormat},
    storage::{OpenDalAccessor, opendal},
};

let root = std::env::current_dir()?;
let operator = opendal::Operator::new(
    opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
)?;
let bytes = OpenDalAccessor::new(operator);
let atif = AtifAccessor::new(bytes, AtifFormat::JsonLines);
let mut reader = atif.open(ReadRequest::new("testdata/atif/trajectories.jsonl")).await?;
while let Some(batch) = reader.next().await? { /* one trajectory per row */ }
# Ok(())
# }
```

`AtifFormat::Json` reads one whole document, including pretty-printed JSON.
`JsonLines` reads one complete trajectory per nonblank line, accepts CRLF and
a final line without a newline, and emits no rows for empty/blank input.
Whole-document JSON requires one valid document; arrays and concatenated documents
are rejected. Extensions do not select a format or compression.

The converter validates ATIF v1.0–v1.8 and uses the fixed
[`atif-arrow` schema](https://github.com/dentiny/atif-arrow/blob/v0.3.0/docs/schema.md).
The schema is available at `open`, before input bytes are read. Each trajectory
is one row with ordered nested steps, calls, observations and supplied metrics.
`raw_json` retains the verbatim document (including its JSONL line ending and
unknown fields). `source_uri` is the request's opaque object locator;
`source_record_index` is the one-based nonblank document index within that object,
reset on each open. Neither field is a synthesized trajectory identifier.
Content text uses UTF-8 bytes in `LargeBinary`; media paths remain strings.
Embedded subagents remain JSON, and external references are not fetched.

`ReadOptions` controls rows per batch and top-level projection in caller order,
including zero-column rows. All ATIF fields are validated, including unselected
fields, but only projected columns are built into Arrow arrays. Omitting `steps`
or `raw_json` avoids accumulating their payloads across the batch. Documents are
buffered in full, and full-schema output retains raw JSON. The row target does not bound document
bytes or total memory. A JSONL read buffers the current document, batch and
one upstream chunk; it does not collect the whole input.

Records retain global input order. Reads are sequential with no detached tasks,
range reads, partition splitting or unordered emission. Dropping the reader stops
work; drop it after cancelling an in-flight decoder call. Decode/validation errors
include the locator and record index, discard the current batch, and terminate
the reader. Input I/O errors retain their category and source chain.

Any sequential byte accessor works, including storage or
`DecompressionLayer::new(Gzip)` for ATIF `.jsonl.gz` objects. The format does not
open OpenDAL operators or implement Harbor Hub discovery. `FileSetAccessor` and
`DirectoryAccessor` compose through their existing ordered read interfaces.

Run the example with either explicit layout:

```sh
cargo run --example read_atif --features format-atif
cargo run --example read_atif --features format-atif -- testdata/atif/trajectories.jsonl jsonl
```

The adapter temporarily pins an `atif-arrow` Git revision containing the
projection-aware builder API with Arrow 59; see
[dependency alignment and release prerequisites](docs/dependencies.md#atif-parser).

## Adding a file format

`FileAccessor<B, F>` binds a byte accessor to a core `ReaderFactory`. Built-in
configuration types are `JsonFormat`, `CsvFileFormat`, `ParquetFormat`,
`XmlFormat`, `WarcFormat` and the existing `AtifFormat` enum. For example,
`FileAccessor::from_format(bytes, JsonFormat::new(schema))` is equivalent to
`JsonAccessor::new(bytes, schema)`. Existing accessor constructors and builders
remain available as type aliases over the shared accessor. `CsvFormat` continues
to configure Arrow CSV delimiter/header options.

Implement `ReaderFactory` for a new single-input format:

1. Set `Request = ReadRequest`, `Reader = dyn RecordReader`, and a typed
   `Prepared` state for an Arrow format. Other output types can use their own
   request and reader interfaces.
2. Implement `prepare(&ReadRequest)` to validate fixed schemas/projections and
   construct per-open decoder state before storage I/O. Formats that inspect
   their input can use `()` as their prepared state.
3. Implement async `create_reader(token, input, request, prepared)` to inspect
   owned input and return a reader with metadata available. Keep format-specific
   batching, projection pushdown and errors in the format.
4. Override `capability(input)` when the default sequential behavior needs more
   input operations, such as exact ranges. Validate actual input capabilities
   and metadata during `create_reader`.

`ReaderFactoryExt::open_reader` runs preparation, opens storage, and invokes
`create_reader`. One blanket implementation supplies it for every factory;
individual factory types cannot override it. Both `FileAccessor` and
decompression invoke this implementation explicitly. Direct factory callers
import `ReaderFactoryExt` to use `factory.open_reader(&storage, request)`.
Conversion from `&Request` to `ByteReadRequest` forwards the exact object and
byte options; the built-in
record and byte requests already support it. A custom single-input request can
implement `From<&CustomRequest> for ByteReadRequest`.

The protocol lives in `open-arrow-access-core` and is re-exported by the facade.
`file_format::FileFormat` remains another name for `ReaderFactory`. Existing
implementations must add the request/reader associated types, change `prepare`
to accept the full request, and rename factory `open` to `create_reader`,
accepting a borrowed `FactoryToken`.
Accessor constructors and builders keep their existing API.

`FactoryToken` is public so other crates can implement the creation hook, but
its construction is private. The blanket `open_reader` implementation supplies
a borrowed token after opening storage. No public constructor, `Default`, or
`Clone` is provided,
so callers without a token cannot directly invoke `create_reader` in safe Rust.

Use `StreamReader` for existing streams, `into_async_read` for futures I/O
parsers, and the [shared format assertions](test-support/README.md#format-contracts)
to test a new Arrow format. Decompression implements the same protocol through
`DecompressionFactory<C>`, while `Codec` supplies the native decoder.

## Caller-configured storage

`storage::{OpenDalAccessor, opendal}` is always available, including with
`--no-default-features`. Wrap a caller-configured SDK operator independently of
any dataset provider. Backend features and credentials stay under the caller's
control.

Generic listing accepts implicit object-store directory prefixes without first
requiring a directory object or `stat` support. The empty prefix selects the
root, and `ListRequest::recursive` controls traversal. Prefix existence and
backend listing errors follow the configured OpenDAL service. In particular, Fs
listing yields no files for a nonexistent or non-directory prefix.
Object and prefix locators must be canonical root-relative paths; aliases such
as leading slashes, dot components, and repeated separators are rejected.

### WARC parser

Enable `format-warc` for `file_format::WarcAccessor`, a thin adapter over the
published `warc-rs` parser. It accepts `ByteReadRequest` and returns one owned
`WarcRecord` per `next()` call, preserving physical record order, record types,
headers and binary bodies. Reader metadata is the input `ObjectRef`.

`WarcAccessor<B>` aliases `FileAccessor<B, WarcFormat>`. `WarcFormat` implements
the same core `ReaderFactory` contract as Arrow formats, using byte requests and
native WARC records. Shared opening handles request forwarding and preparation;
the parser retains its own record conversion and errors.

This stage provides parser records, without an Arrow schema, Arrow batches,
projection, or batch-size controls. Arrow conversion, record locators and generated
columns are deferred. Supply external decompression separately. The parser buffers
one complete body per record, so this is not a maximum-record-size or memory bound.
