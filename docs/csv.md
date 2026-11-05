# CSV and TSV

Enable the default-off `format-csv` feature. The same reader handles CSV, TSV
and other byte delimiters through Arrow CSV's `Format`, re-exported as `CsvFormat`.
The implementation uses [`arrow-csv` 59.3.0](https://docs.rs/arrow-csv/59.3.0/arrow_csv/reader/index.html)
for parsing and Arrow conversion; it does not implement another CSV parser.

```rust,no_run
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    file_format::{CsvAccessor, CsvFormat},
    storage::FsAccessor,
};

async fn read() -> Result<()> {
    let files = FsAccessor::new("/absolute/data/root").await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
    ]));
    let format = CsvFormat::default()
        .with_header(true)
        .with_header_validation(true)
        .with_delimiter(b'\t');
    let csv = CsvAccessor::new(files, schema).with_format(format);
    let mut reader = csv.open(ReadRequest::new("data.tsv")).await?;
    while let Some(batch) = reader.next().await? {
        // Consume the ordered batch.
    }
    Ok(())
}
```

The example requires `storage-fs,format-csv` and an `arrow-schema = "=59.3.0"`
dependency. Any shared sequential byte accessor can replace the filesystem;
range access and a known input length are unnecessary.

## Schema and dialect

Supply the complete physical input schema in column order, with at least one
field. The schema is available immediately after `open`; this feature does not
infer types or guess headers from input. Defaults are comma-delimited input
without a header. Header handling skips the first record; enable header
validation to check its names against the supplied schema.

`CsvFormat` provides Arrow's quote, escape, comment, terminator, null-regex and
truncated-row settings. By default, empty fields are null and row widths must
match the physical schema. Arrow's type conversions and permissive CSV quoting
rules apply, including its handling of unterminated quotes. This adapter does
not add a strict RFC validator. Multiline quoted values, escaped quotes, UTF-8
split across chunks and a final record without a newline are supported.

`ReadRequest::options` controls batch size and top-level projection. Projection
preserves requested field order and schema metadata; an empty projection retains
row counts. Unknown and repeated projection names fail before opening storage.
Projected-away fields are structurally parsed but their types are not validated.
Unsupported Arrow input types fail during decoding when selected data is read.

## Streaming and errors

`open` forwards `ByteReadOptions` to the byte accessor without reading CSV data.
Each `next` pulls only enough chunks to produce the next batch. The reader keeps
one unconsumed input chunk and Arrow's current batch/partial record; it does not
collect the file or start producer tasks. Batch size bounds row count, not the
bytes in a single record, the upstream chunk size or Arrow's retained buffers.

Empty input and header-only input emit no batches. Parsing, width, type and UTF-8
failures return `ErrorKind::Decode` with the Arrow error as their source. Byte
I/O failures retain their original category and context. EOF and errors are
terminal; subsequent calls return `None` without polling the byte source again.
Output remains sequential and ordered. External decompression and parallel
record scans are separate features.
