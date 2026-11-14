# Using the library for pretraining data preparation

Use these recipes to build the ingestion stage of a pretraining data preparation
job: read raw corpus inputs, decode records, and hand Arrow batches to your
preprocessing stages. Cleaning, filtering, deduplication, tokenization, and sample
packing run in your downstream pipeline.

Build a reader by choosing storage, an optional byte transformation, and a format.
Use a dataset accessor when the input spans multiple files, or a provider when
source discovery needs more than a storage prefix.

## Install and read a file

The [README quick start](../README.md#quick-start) contains a complete filesystem
Parquet program and its dependencies. Examples here use the same async context
and `Result` error propagation. Enable each example's listed facade features;
backend features belong on your OpenDAL dependency.

From a repository checkout, this command reads a checked-in fixture:

```sh
cargo run --example read_parquet --features format-parquet -- testdata/parquet ordered-snappy.parquet
```

Paths passed to generic storage are relative to the operator's configured root.
Choose the format explicitly; extensions do not select a decoder.

## Select columns and batch rows

With `format-parquet`, use the same `testdata/parquet` root as the fixture command
above. Configure an existing `parquet` accessor before opening:

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_access::{Accessor, ByteReadOptions, ReadOptions, ReadRequest};

let request = ReadRequest::new("ordered-snappy.parquet")
    .with_options(ReadOptions {
        batch_size: NonZeroUsize::new(4096).unwrap(),
        projection: Some(vec!["title".into(), "id".into()]),
    })
    .with_byte_options(ByteReadOptions {
        io_concurrency: NonZeroUsize::new(4).unwrap(),
        io_chunk_size: NonZeroUsize::new(4 * 1024 * 1024),
    });
let mut reader = parquet.open(request).await?;
while let Some(batch) = reader.next().await? {
    println!("{} rows; columns: title, id", batch.num_rows());
}
```

Projection uses top-level field names, preserves requested order, and rejects
unknown or duplicate names. `None` selects all fields; `Some(vec![])` retains
row counts with zero columns. The default row target is 8,192. It is not a byte
or total-memory limit.

Byte concurrency and splitting tune storage I/O for each reader. They preserve
output order and do not enable multi-file parallel scans. Multiple independently
opened readers have separate budgets. `OpenDalAccessor::with_chunk_size` controls
emitted byte chunks, independently of both settings above.

To use a standard futures stream, pass an owned record reader to
`open_arrow_access::into_stream(reader)`. It yields `Result<RecordBatch>` and
terminates after EOF or the first error.

## Use S3 or another OpenDAL backend

Enable the upstream backend and transport features alongside your chosen format:

```toml
[dependencies]
open-arrow-access = { version = "0.1.0", features = ["format-parquet"] }
opendal = { version = "0.59.2", default-features = false, features = ["services-s3", "http-transport-reqwest"] }
```

Configure the operator in your application, then wrap it using the same accessor:

```rust,no_run
use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::parquet::ParquetAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read(operator: opendal::Operator) -> open_arrow_access::Result<()> {
    let files = OpenDalAccessor::new(operator);
    let parquet = ParquetAccessor::new(files);
    let mut reader = parquet.open(ReadRequest::new("corpus/part-00000.parquet")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}

fn operator() -> Result<opendal::Operator, opendal::Error> {
    // Configure credentials, endpoints, and access policies for your deployment.
    opendal::Operator::new(
        opendal::services::S3::default()
            .bucket("my-corpus")
            .region("us-east-1"),
    )
}
```

Create/configure the operator before wrapping it; complete synchronous backend
initialization before entering an async task. The same approach applies to other
OpenDAL services. A service must provide the operations required by the decoder;
see [storage requirements](support.md#storage).

## Read compressed JSONL

Enable `format-json` and `decompression-gzip`. JSON/CSV require an explicit Arrow
schema; add `arrow-schema = "58.4.1"` to construct its fields. This complete reader
function accepts a caller-configured operator:

```rust,no_run
use std::sync::Arc;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, AccessorExt, ReadRequest,
    decompression::{DecompressionLayer, gzip::Gzip},
    file_format::json::JsonAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read(operator: opendal::Operator) -> open_arrow_access::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let compressed = OpenDalAccessor::new(operator);
    let decoded = compressed.layer(DecompressionLayer::new(Gzip));
    let json = JsonAccessor::new(decoded, schema);
    let mut reader = json.open(ReadRequest::new("corpus/part-00000.jsonl.gz")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

This expects objects such as `{"id":"doc-1","text":"..."}`, one per line or
otherwise separated by whitespace. Top-level arrays are not expanded. Missing
nullable fields become null; unknown fields are ignored. Read to EOF to complete
gzip integrity checks.

Replace the format accessor for other sequential inputs:

| Input | Construction | Additional feature |
| --- | --- | --- |
| CSV with headers | `CsvAccessor::new(decoded, schema).with_format(CsvFormat::default().with_header(true))` | `format-csv` |
| TSV | Same CSV accessor with `.with_delimiter(b'\t')` on `CsvFormat` | `format-csv` |
| XML | `XmlAccessor::new(decoded, "/records/record")` | `format-xml` |
| ATIF JSONL | `AtifAccessor::new(decoded, AtifFormat::JsonLines)` | `format-atif` |

Use `CsvAccessor::infer(decoded)` to infer CSV column types and header presence.
Configure sampling and override ambiguous headers with `CsvInferenceOptions`.

Use `JsonAccessor::infer(decoded)` to infer a JSON schema rather than supplying
one. Configure the bounded sample with `JsonInferenceOptions`.

Import each type from its `file_format::<format>` module. XML infers a schema at
open unless explicit mappings are supplied; ATIF uses its fixed trajectory schema.
Consult [format behavior](formats.md) before choosing schema and record limits.
External gzip cannot directly feed Parquet because it removes range access.

## Read multiple shards

Enable `dataset-directory` and the format feature. Use `FileSetAccessor` for a
manifest's declared order:

```rust,no_run
use open_arrow_access::{
    Accessor,
    dataset::file_set::{FileSetAccessor, FileSetRequest},
    file_format::parquet::ParquetAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read(operator: opendal::Operator) -> open_arrow_access::Result<()> {
    let parquet = ParquetAccessor::new(OpenDalAccessor::new(operator));
    let dataset = FileSetAccessor::new(parquet);
    let mut reader = dataset.open(FileSetRequest::new([
        "train/part-00000.parquet",
        "train/part-00001.parquet",
    ])).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

For prefix discovery, replace the selection with:

```rust,ignore
use open_arrow_access::dataset::directory::{DirectoryAccessor, DirectoryRequest};

let bytes = OpenDalAccessor::new(operator);
let parquet = ParquetAccessor::new(bytes.clone());
let dataset = DirectoryAccessor::new(bytes, parquet);
let mut request = DirectoryRequest::new("train");
request.suffix = Some(".parquet".into());
request.recursive = false;
let reader = dataset.open(request).await?;
```

Directories sort and deduplicate selected paths; explicit file sets preserve
order and duplicates. Both read files lazily and sequentially. The first file
fixes the output schema by default, and later files must match the projected
schema, including metadata. An empty selection is invalid. With inferred XML,
per-file samples can produce incompatible schemas; explicit mappings provide a
stable multi-file contract, or opt into union by name as described below.

Both request types expose `with_options` and `with_byte_options`. Discovery needs
listing capability; file sets can work without it.

To union missing/reordered columns by name instead of requiring identical schemas,
add `.with_union_by_name()` to the FileSet or Directory accessor. Union by name also
handles nested Struct/List fields and nullability. It performs a sequential schema
preflight, then reopens files lazily; sources must remain stable. Projection is
applied against the union rather than each file. See the [union-by-name contract](../dataset/README.md#union-by-name)
for type/metadata conflict rules and resource behavior.

## Read a Hugging Face split

Enable `service-huggingface`; it brings in the provider's internal Parquet and
file-set dependencies. No local OpenDAL backend feature is required for this path.

```rust,no_run
use open_arrow_access::{
    Result,
    service::huggingface::{
        HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceService,
    },
};

async fn read() -> Result<()> {
    let service = HuggingFaceService::new(HuggingFaceOptions::default()).await?;
    let request = HuggingFaceDatasetRequest::new("lhoestq/demo1", "default", "train");
    let mut reader = service.dataset(request).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

The service pins the Parquet export revision to a commit, lists one config/split,
sorts shards, and reuses the Parquet/FileSet readers. `with_revision` selects the
export revision, not a mapping from an original source revision. Missing full
exports fail unless `with_partial_exports()` explicitly permits fallback.
`with_options` and `with_byte_options` configure the scan.

Use `repository(...)` when you need original repository files and want to choose
the format yourself. Floating repository revisions are not snapshots. See the
[provider guide](../service/huggingface.md).

## Work with archives and other providers

ZIP and TAR return entry bytes rather than automatically selecting a format.
Use the [ZIP guide](../decompression/README.md#zip-entries) or
[TAR example](../examples/read_tar.rs) to inspect/select members, then feed entry
readers to sequential consumers. `TarStream` can follow gzip for `.tar.gz`;
`ZipArchive` and `TarArchive` require ranged, known-length inputs.

An archive entry is an already-open `ByteReader`, whereas `JsonAccessor::new`
expects a byte accessor that can open objects. Use a format factory's
`ReaderFactoryExt::open_input` to decode entry bytes directly. With `format-json`:

```rust,no_run
use open_arrow_access::{
    ByteReader, ReadRequest, ReaderFactoryExt, Result, SchemaRef,
    file_format::json::JsonFormat,
};

async fn read_json_entry(input: Box<dyn ByteReader>, schema: SchemaRef) -> Result<()> {
    let format = JsonFormat::new(schema);
    let mut reader = format.open_input(
        input,
        ReadRequest::new("corpus.tar#records.jsonl"),
    ).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

Pass the selected body from `TarStream::next_entry`, `TarArchive::open_entry`, or
`ZipArchive::open_entry` as `input`. Choose an entry whose bytes match the decoder
and provide its schema. The request's locator is a logical source label for the
record reader; `open_input` does not reopen that path or reconfigure the input's
storage I/O. Replace the example label with your archive/member identity.

Other providers may return metadata pages, lazy file accessors, Arrow metadata
rows, or decoded bytes. Start with the [provider matrix](support.md#dataset-providers),
then follow its guide. Discovery does not imply that a provider downloads article
content or supplies a text-extraction pipeline.

## Handle lifecycle and source identity

- Obtain the schema from `reader.metadata().schema` after open and before reading.
- Treat errors and EOF as terminal. After cancelling an in-flight decoder call,
  drop the reader; do not assume it can resume.
- Drop a reader to release its owned input and decoder state.
- Keep source objects unchanged for the scan. Record revision/release identity and
  provider metadata separately when building a reproducible corpus manifest.
- Size row targets, byte chunks, and concurrent readers for your workload. A
  single record, trajectory, or Parquet row group may exceed those target sizes.

See [architecture](architecture.md) for ownership and concurrency semantics and
[API contracts](contracts.md) for custom stream and reader adapters.
