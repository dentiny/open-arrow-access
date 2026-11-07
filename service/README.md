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

## PMC article metadata

`pmc::PmcAccessor` reads metadata JSON from the
[PMC Article Datasets](https://pmc-oa-opendata.s3.amazonaws.com/README.txt), using
a byte accessor rooted at `pmc-oa-opendata` or a mirror preserving its metadata
paths. Each row contains mapped article metadata, the original PDF URL, and
verbatim source JSON. It never opens a PDF object, checks PDF availability, or
reads PDF contents.

```rust,no_run
use open_arrow_access::{
    Accessor,
    service::pmc::{PmcAccessor, PmcRequest},
    storage::{OpenDalAccessor, opendal},
};

async fn read() -> Result<(), Box<dyn std::error::Error>> {
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root("/data/pmc-mirror"),
    )?;
    let source = PmcAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = source.open(PmcRequest::new("metadata/PMC123.2.json")).await?;
    while let Some(batch) = reader.next().await? {
        assert_eq!(batch.num_columns(), 8);
        // source_uri contains the PDF URL; PDF bytes are not downloaded.
    }
    Ok(())
}
```

For a downstream filesystem mirror, enable OpenDAL's `services-fs` backend on
your OpenDAL dependency. Within this repository, enable `service-pmc` and run:

```sh
cargo run --example read_pmc --features service-pmc -- /data/pmc-mirror metadata/PMC123.2.json
```

`PmcRequest::files([...])` reads explicit metadata objects in caller order and
retains duplicates. `PmcRequest::prefix("metadata/")` streams recursive `.json`
discovery in backend order, without collecting or sorting all paths. For
nonrecursive listing, set `recursive = false` on the request's
`PmcInput::Prefix(ListRequest)`. Empty selections expose the schema but emit no
batches. `open` validates options and starts listing if requested; `next` reads
metadata lazily. Dropping the reader stops discovery and reading.

For the public S3 bucket, wrap a caller-configured OpenDAL SDK operator. Enable
upstream `services-s3` and `http-transport-reqwest` features on your OpenDAL
dependency, as described in the [storage example](../README.md#quick-start):

```rust,ignore
opendal::install_default();
let operator = opendal::Operator::new(
    opendal::services::S3::default()
        .bucket("pmc-oa-opendata")
        .region("us-east-1")
        .skip_signature(),
)?;
let source = PmcAccessor::new(open_arrow_access_storage::OpenDalAccessor::new(operator));
let reader = source.open(PmcRequest::prefix("metadata/")).await?;
```

The provider adds no transport. Byte options are forwarded to metadata opens.
Byte layers, including decompression, can wrap the supplied accessor; apply them
explicitly to compatible metadata mirrors. Limits apply to decoded bytes.

### Schema and mapping

`pmc::schema()` returns these eight columns in order, with schema metadata
`pmc_arrow.row_kind = article_metadata`:

| Column | Arrow type | Nullable | Mapping |
| --- | --- | --- | --- |
| `pmcid` | Utf8 | No | Canonicalize `pmcid`, e.g. ` pmc00123 ` → `PMC123` |
| `pmc_version` | UInt32 | Yes | Positive `version`; unknown remains null |
| `pmid` | Utf8 | Yes | String or unsigned integer PMID; integer becomes decimal text |
| `doi` | Utf8 | Yes | `doi` |
| `title` | LargeUtf8 | Yes | `title` |
| `license` | Utf8 | Yes | `license_code` |
| `source_uri` | Utf8 | No | Original `pdf_url`, including query/fragment |
| `raw_metadata_json` | LargeUtf8 | Yes | Complete original JSON, including whitespace and unknown fields |

Metadata must be exactly one valid UTF-8 JSON object. Missing/null `pdf_url`
continues to skip the article. Malformed mapped fields are terminal errors.
Empty optional strings stay empty; duplicate JSON keys use the last value.
Unknown fields, including numbers outside the f64 range, remain in raw JSON.
Scalar fields are normalized into bounded JSONL batches and converted to Arrow
by `file_format::JsonAccessor`; only raw JSON uses a dedicated array builder.

PDF URLs are checked locally against `s3://pmc-oa-opendata/` and canonical path
rules. The URL is preserved verbatim. Percent decoding is used only for syntax
validation; it does not select or open another object. PDF existence, contents,
headers, and MD5 query parameters are not checked. A mirror only needs metadata
JSON objects; PDFs do not need to be present.

### Options, limits and errors

`PmcRequest` implements `ReadOptionsProvider`. Projection addresses the final
eight-column schema, keeps caller field order and schema metadata, and preserves
row counts for an empty projection. It is validated before listing or reads.
All selections, including zero-column projections, read only metadata JSON.

The common nonzero batch row target is an upper bound. `PmcLimits` adds a hard
metadata-object limit (4 MiB by default) and a soft 64 MiB batch payload target.
The payload count includes raw and normalized metadata bytes, checked after each
row; it may exceed the target by one bounded metadata object. This is not an RSS
limit: upstream chunks, decoder state, temporary objects, and Arrow allocations
also consume memory. Known lengths are checked before reading; streamed bytes
are checked incrementally. Exceeding a limit is a terminal decode error.

Files and records remain sequential and ordered. Batches may be smaller than
the row target. A failure discards the current unreturned batch, terminates the
reader, and retains underlying I/O error categories and source chains. Previously
returned batches remain delivered; no file is transparently replayed. Discovery
order is not a stable snapshot.

PDF/XML/text/media retrieval, OCR, extraction, source inventory snapshots,
parallel scans, indexes, and writers are separate capabilities. The
[source-attribute proposal](../docs/source-attributes.md) remains a future
planning extension.
