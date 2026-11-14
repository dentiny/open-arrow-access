# Format behavior

Use the [support matrix](support.md#file-formats) to choose a decoder and the
[usage guide](usage.md) to compose it with storage. Format selection is explicit;
filename extensions do not attach parsers or decompression layers.

## Parquet

`ParquetAccessor::new(bytes)` reads the file schema at open and requires exact
byte ranges and a known input length. Projection is pushed into the Parquet
reader. Uncompressed and Snappy pages are supported by `format-parquet`.
Enable `format-parquet-brotli` for Brotli, `format-parquet-gzip` for Gzip, `format-parquet-lz4` for LZ4, `format-parquet-zstd` for Zstandard
compressed pages; each codec feature also enables the Parquet reader. External file compression is separate:
a gzip byte layer removes the range access Parquet needs.

## JSON and JSONL

`JsonAccessor::new(bytes, schema)` uses an explicit Arrow schema.
`JsonAccessor::infer(bytes)` infers a fixed schema at open from up to 1000
objects or 64 MiB of sampled input. Configure these targets with
`with_inference_options(JsonInferenceOptions { max_records, max_bytes })`.
Targets are checked between complete objects: a single object and one upstream
chunk can exceed the byte target. Sampling and decoding share one input, so
sequential and compressed sources do not need to be reopened. Sampled bytes,
including lookahead, are replayed before the remaining input. Arrow supplies
nested Struct/List inference and type merging within the sample. Bare
`NaN`, `Infinity`, and `-Infinity` infer as Float64; quoted strings remain strings. Empty input
requires an explicit schema. Projection is resolved after inference; schema
inference is independent for each file, and later values use the fixed schema.
Native parallel scans infer once before partitioning, then share that schema
across every decode partition. It accepts
whitespace-separated objects, including JSONL and pretty-printed object streams.
Top-level arrays are not flattened; arrays, scalars and null records are rejected.
Empty or whitespace-only input emits no rows. Missing nullable fields become
null and unknown fields are ignored. Decoding follows Arrow JSON's permissive
semantics; this adapter adds no strict JSON validator.

Rows are decoded incrementally. Decoder errors, including incomplete records at
EOF, terminate the reader. Batch size does not bound one record's byte size.

## CSV and TSV

`CsvAccessor::new(bytes, schema)` uses an explicit Arrow schema, while
`CsvAccessor::infer(bytes)` infers column types. Both default to
`CsvHeader::Auto`; override either with `.with_header(CsvHeader::Present)` to
consume the first record, or `.with_header(CsvHeader::Absent)` to keep every
record as data. The policy is independent of schema-sampling options.

With an explicit schema, Auto consumes the first record only when every field
exactly matches the full schema's column names in order, before projection.
Case, whitespace, field count, and unselected columns matter. A mismatched row
remains data, so conversion errors are reported rather than used as evidence
for a header. All-string data is preserved unless it exactly matches the names;
use Absent if such a matching row is actually data. Empty and matching header-only
inputs emit no rows. Auto inspects one complete record during `open`, so source
errors may surface there; explicit Present/Absent do not require this inspection.

Without an explicit schema, Auto compares the first record with types inferred
from subsequent records. Text labels above consistently typed data indicate a
header. Numeric-looking text such as `+1` or integers beyond i64, numeric widening,
and later nulls alone do not indicate a header. Ambiguous all-string and single-row
samples are retained as data with `column_1`, `column_2`, etc. Use Present for
all-string files that have headers. This conservative policy follows arrow-csv's
[`infer_format` approach](https://github.com/apache/arrow-rs/blob/main/arrow-csv/src/reader/mod.rs); schema inference and decoding use the project's Arrow 58
dependency.

The default schema sample includes up to 1000 complete records (including the
potential header) or 64 MiB of canonical CSV bytes. Configure these targets with
`with_inference_options(CsvInferenceOptions { max_records, max_bytes })`.

Inference uses the source `CsvDialect`, preserving quoted delimiters, multiline
fields, custom quotes/escapes, comments, terminators, UTF-8 and CRLF.
`CsvDialect::default().header` is Auto, so changing only the delimiter preserves
detection. Set `CsvDialect { header: CsvHeader::Present, ..Default::default() }`
for a known header, or call `with_header` after `with_dialect` to override its policy.
Sampling options do not change header handling. Opaque `with_format` settings
require an explicit schema and retain their explicit boolean header setting;
Auto requires `with_dialect` and is rejected with opaque formats before storage I/O.
Record framing reuses the native scan parser and canonical quoting; sampled records and unread input
continue through one source reader without reopening storage, including gzip
streams. Byte targets are soft and checked between complete encoded records;
one record, parser lookahead and upstream chunks can exceed them.

The schema is fixed before returning the reader. Projection is resolved after
inference, and native parallel scans share the inferred schema across partitions.
Schemas are inferred separately per file; no cross-file reconciliation is added.
Later incompatible values fail decoding. Empty inputs, samples explicitly marked
Present that contain only a header, all-null sampled columns, and duplicate/empty
headers require an explicit schema.
For identifiers with significant leading zeros, supply an explicit string schema.
Configure TSV with `with_dialect(CsvDialect { delimiter: b'\t', ..Default::default() })`.
Header policy is resolved once before parallel partitioning and shared by all workers.
`CsvFormat` is this crate's options wrapper around Arrow 58's CSV format, with
the existing builder methods and `.with_header_validation(true)` preserved.
It is no longer a type alias for `arrow_csv::reader::Format`. Header validation
checks the complete source schema before projection, using the same incremental
CSV parser and dialect. Data still uses Arrow CSV's incremental decoder over
sequential bytes.

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

The adapter uses the published `atif-arrow` 0.5 release series with Arrow 58.


## WARC parser

Enable `format-warc` for `file_format::warc::WarcAccessor`, a thin adapter over the
published `warc-rs` parser. It accepts `ByteReadRequest` and returns one owned
`WarcRecord` per `next()` call, preserving physical record order, record types,
headers and binary bodies. Reader metadata is the input `ObjectRef`.

`WarcAccessor<B>` aliases `FileAccessor<B, WarcFormat>`. `WarcFormat` implements
the same core `ReaderFactory` contract as Arrow formats, using byte requests and
native WARC records. Shared opening handles request forwarding and preparation;
the parser retains its own record conversion and errors.

The native accessor provides parser records without Arrow projection or batch-size
controls. To select columns, use `WarcArrowAccessor` with a `ReadRequest` instead:

```rust,ignore
use open_arrow_access::{Accessor, ReadOptions, ReadRequest};
use open_arrow_access::file_format::warc::WarcArrowAccessor;

// `bytes` is any sequential byte accessor, such as OpenDalAccessor.
let warc = WarcArrowAccessor::new(bytes);
let request = ReadRequest::new("part.warc").with_options(ReadOptions {
    projection: Some(vec![
        "record_id".into(),
        "record_type".into(),
        "date".into(),
        "target_uri".into(),
        "content_type".into(),
    ]),
    ..Default::default()
});
let mut reader = warc.open(request).await?;
while let Some(batch) = reader.next().await? {
    // Exactly the five requested columns, in caller order; no body Arrow array.
}
```

The fixed schema has eight top-level fields:

| Field | Arrow type | Nullable |
| --- | --- | --- |
| `record_id` | Utf8 | No |
| `record_type` | Utf8 | No |
| `warc_version` | Utf8 | No |
| `date` | Timestamp(Millisecond, None) | No |
| `source_uri` | LargeUtf8 (input locator) | No |
| `target_uri` | LargeUtf8 | Yes |
| `content_type` | Utf8 | Yes |
| `body` | LargeBinary | No |

The default Arrow reader applies `SchemaEvolutionLayer` to convert parsed RFC 3339
dates to UTC epoch milliseconds, stored without timezone metadata. Sequential and
parallel scans expose the same timestamp schema, including projected reads.
Sub-millisecond precision is not retained. Native WARC records
retain the parser's date representation.

Each physical WARC record produces one row, including warcinfo and continuation
records. Bodies retain their original bytes; embedded HTTP messages are not
extracted or decoded, target URIs are not fetched, and revisits are not resolved.
Use the native accessor for headers outside this schema. `projection: None`
selects all eight fields; an empty projection preserves row counts with zero
columns. Unknown and duplicate field names fail before source I/O.

Projection avoids building Arrow arrays for omitted fields. The parser still
reads, buffers and validates complete physical records, including unselected
bodies; omitting `body` does not avoid downloading it or impose a body-size limit.
`batch_size` targets rows, not bytes or total memory. Arrow decoding also supports
`plan_scan` and `ParallelReadLayer`; see [WARC partitions](../file-format/README.md#warc-arrow-partitions).

Supply external decompression separately. For `.warc.gz`, construct the byte
accessor with `bytes.layer(DecompressionLayer::new(Gzip))` before passing it to
`WarcArrowAccessor::new`, importing `AccessorExt` and enabling
`decompression-gzip`. Decompression and WARC framing remain sequential.
