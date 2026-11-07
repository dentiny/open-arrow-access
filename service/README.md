# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. Each
provider adds its own default-off feature and facade exports.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
Generic storage access and object-store prefix listing belong in `storage`,
not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.

## PMC article-version metadata (`pmc`)

Enable `service-pmc` for `service::pmc::PmcAccessor`. This service reads exactly
one metadata JSON object named by a standard `ReadRequest`, and emits one row.
It never lists a directory or opens a PDF, XML, text, or media URL. Inventory
enumeration is a separate service. The input byte accessor requires sequential
reads only; filesystem, SDK operators, and decompression layers are caller-owned.

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, service::pmc::PmcAccessor};
use open_arrow_access_storage::{OpenDalAccessor, opendal};

async fn read(operator: opendal::Operator) -> Result<(), Box<dyn std::error::Error>> {
    let source = PmcAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = source.open(ReadRequest::new("metadata/PMC123.2.json")).await?;
    while let Some(batch) = reader.next().await? {
        assert_eq!(batch.num_rows(), 1);
    }
    Ok(())
}
```

For a local mirror, run `cargo run --example read_pmc --features service-pmc --
/data/pmc-mirror metadata/PMC123.2.json`. For multiple metadata objects, compose
with the existing file-set dataset or open the explicit keys supplied by an
inventory reader. There is no provider-specific `Files` or `Prefix` input.

### Article metadata contract

The schema uses the upstream names: `pmcid`, `version`, `pmid`, `doi`, `title`,
`citation`, `is_pmc_openaccess`, `is_manuscript`, `is_historical_ocr`,
`is_retracted`, `license_code`, `xml_url`, `pdf_url`, `media_urls`, and `text_url`.
`raw_metadata_json` contains the complete verbatim UTF-8 object, preserving
unknown fields, large numbers, whitespace, and duplicate keys. Known duplicate
keys use their final value for typed columns. Reader metadata records the input
object as its source; schema metadata records `pmc.row_kind = article_metadata`.

`pmcid` is required and validated, but its source spelling is retained. `version`
is nullable UInt32 and must be positive when present. Numeric PMIDs become
Utf8 decimal strings; the raw JSON retains the original numeric representation.
`title` and `citation` are LargeUtf8, the four `is_*` fields are nullable Boolean,
`media_urls` is a nullable List<Utf8>, and the other optional fields are nullable
Utf8. A missing or null PDF URL is a normal row with a null `pdf_url`. No article
is filtered by content availability, licence, language, or duplication. Content
URL strings are retained as supplied and are never interpreted as read requests.

Validation lives in `pmc/validation.rs`. Invalid JSON or mapped field types are
terminal decode errors. The default hard decoded metadata limit is 4 MiB;
configure it through `PmcAccessor::with_limits(PmcLimits { metadata_bytes })`.
Known lengths and streamed chunks are checked. Empty upstream byte chunks are
I/O errors. `open` validates projection without starting a read; locator syntax
is owned by the underlying byte accessor and passed through unchanged.
`next` performs one metadata read. Errors and EOF are terminal; dropping a pending
reader releases its input. Common byte options are forwarded unchanged.

Projection supports caller field order and zero-column row counts. The schema
is fixed before reading, and every valid metadata object produces one row.
Known fields are validated even when omitted from the projection. Only selected
Arrow columns are built directly from the source fields, without serializing or
decoding an intermediate JSON object. The Arrow schema is the single field/type
definition: conversion dispatches on its data types, with special rules only for
PMCID, PMID, and version. There is no intermediate article struct to maintain.
The JSON file-format feature is not required.
The row target is an upper bound; this single-object service emits at most one
batch with one row. It does not fetch an inventory, choose a snapshot, filter
licences, extract text, or write Lance datasets.

The article reader uses core `read_limited` for byte bounds and `StreamReader`
for terminal handling. JSON conversion and article validation stay in PMC.
