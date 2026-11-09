# Service response shapes

## Service and data

Services read different kinds of source data; they do not share a universal
record schema. This guide describes the library response families. Each
[provider guide](README.md#service-and-data) documents its actual input fields,
file-name mapping and output columns.

## Request and response

| Family | Request / response shape | Data retained |
| --- | --- | --- |
| Parsed API metadata | Provider method → `ApiDocument`; `value() -> &serde_json::Value` | Complete parsed JSON for that API response |
| Named original files | Provider resolution → `ResolvedFileSet<A, M>`; `list("")` → names; `open(ByteReadRequest)` → byte reader | Documented metadata `M` and lazy file mappings |
| One-object Arrow metadata | `ReadRequest { object, options, byte_options }` → record reader | Fixed schema from [PMC](pmc.md) or [Gutenberg](gutenberg.md) |
| Multi-file Arrow data | Provider-specific request → record reader | Manifest schema for [PMC inventory](pmc-inventory.md), first-shard schema for [Hugging Face](huggingface.md), caller format schema for [Common Crawl](commoncrawl.md) |
| XML harvest page | `records` / `resume` → `OaiPage { bytes, record_count, resumption_token }` | Exact XML, including deleted record headers |
| Archive members | Archive session + entry index → byte reader | Manifest/directory metadata and selected decoded bytes |

```text
ByteReader:
  metadata() -> &ByteMetadata { source?, content_length?, version? }
  next().await -> Result<Option<Bytes>>

RecordReader:
  metadata() -> &RecordMetadata { schema: SchemaRef, source: Option<ObjectRef> }
  next().await -> Result<Option<RecordBatch>>
```

`source` describes the input object, not an extra Arrow column. Its value can be
None for multi-file readers and direct sequential HTTP responses. Length can be
unknown after decompression. ETag/version metadata is not itself a snapshot promise.

`ReadOptions` contains a nonzero target batch size and optional top-level field
names in requested order. Empty projection preserves row counts with zero
columns. Reader metadata is available after open; all emitted batches match its
schema. The full schemas and nullability live alongside the provider examples:
[PMC](pmc.md#request-and-response), [Gutenberg](gutenberg.md#request-and-response),
[PMC inventory](pmc-inventory.md#request-and-response),
[Hugging Face](huggingface.md#request-and-response).

Zenodo/Dataverse/Internet Archive expose typed file metadata subsets. Other
JSON-backed named-file providers generally retain an `ApiDocument`; check the
guide before assuming raw upstream JSON is accessible through `metadata()`.
CourtListener specifically returns decoded CSV bytes, not Arrow batches.

## Usage and results

```rust
use open_arrow_access::{RecordReader, Result};

async fn inspect_first_batch(reader: &mut dyn RecordReader) -> Result<()> {
    println!("declared schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        for field in batch.schema().fields() {
            println!(
                "{}: {:?}, nullable={}",
                field.name(),
                field.data_type(),
                field.is_nullable()
            );
        }
        println!("rows: {}", batch.num_rows());
    }
    Ok(())
}
```

Pass an opened record reader from a provider guide. The example reports actual
field names, Arrow types, nullability and first-batch row count. It does not
invent columns such as repository IDs or attach file contents to metadata rows.
For byte responses, use `ByteReader::next()` and apply a matching decoder.
