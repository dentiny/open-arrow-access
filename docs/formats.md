# Format behavior

Use the [support matrix](support.md#file-formats) to choose a decoder and the
[usage guide](usage.md) to compose it with storage. Format selection is explicit;
filename extensions do not attach parsers or decompression layers.

## Parquet

`ParquetAccessor::new(bytes)` reads the file schema at open and requires exact
byte ranges and a known input length. Projection is pushed into the Parquet
reader. Uncompressed and Snappy pages are supported by `format-parquet`.
Enable `format-parquet-zstd` for Zstandard-compressed pages; it also enables
the Parquet reader. External file compression is separate:
a gzip byte layer removes the range access Parquet needs.

## JSON and JSONL

`JsonAccessor::new(bytes, schema)` requires a fixed Arrow schema. It accepts
whitespace-separated objects, including JSONL and pretty-printed object streams.
Top-level arrays are not flattened; arrays, scalars and null records are rejected.
Empty or whitespace-only input emits no rows. Missing nullable fields become
null and unknown fields are ignored. Decoding follows Arrow JSON's permissive
semantics; this adapter adds no strict JSON validator.

Rows are decoded incrementally. Decoder errors, including incomplete records at
EOF, terminate the reader. Batch size does not bound one record's byte size.

## CSV and TSV

`CsvAccessor::new(bytes, schema)` requires a fixed Arrow schema. Configure headers
with `CsvFormat::default().with_header(true)` and TSV with `.with_delimiter(b'\t')`.
The reader uses Arrow CSV's incremental decoder over sequential bytes.

## XML

Enable `format-xml` and supply the input accessor and absolute record element path.
The output schema is inferred automatically. The filesystem examples on this page
also need OpenDAL `services-fs` and Tokio, as in the [quick start](../README.md#quick-start):

```rust,no_run
use open_arrow_access::{
    Accessor, ReadRequest, file_format::xml::XmlAccessor,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?.join("testdata/xml");
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let xml = XmlAccessor::new(OpenDalAccessor::new(operator), "/inventory/warehouse");
    let mut reader = xml.open(ReadRequest::new("inventory.xml")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
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
use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::atif::{AtifAccessor, AtifFormat},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let bytes = OpenDalAccessor::new(operator);
    let atif = AtifAccessor::new(bytes, AtifFormat::JsonLines);
    let mut reader = atif.open(ReadRequest::new("testdata/atif/trajectories.jsonl")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} trajectories", batch.num_rows());
    }
    Ok(())
}
```

`AtifFormat::Json` reads one whole document, including pretty-printed JSON.
`JsonLines` reads one complete trajectory per nonblank line, accepts CRLF and
a final line without a newline, and emits no rows for empty/blank input.
Whole-document JSON requires one valid document; arrays and concatenated documents
are rejected. Extensions do not select a format or compression.

The converter validates ATIF v1.0–v1.8 and uses the fixed
[`atif-arrow` schema](https://github.com/dentiny/atif-arrow/blob/v0.4.0/docs/schema.md).
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

The adapter uses published `atif-arrow 0.4.0` with Arrow 59; see
[dependency alignment](dependencies.md#atif-parser).


## WARC parser

Enable `format-warc` for `file_format::warc::WarcAccessor`, a thin adapter over the
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
