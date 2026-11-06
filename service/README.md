# Dataset source providers

Enable `service-commoncrawl` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Crawl-wide WARC datasets and segment selection are separate additions.

Providers reuse the common byte/record accessor contracts and existing format
readers. Each provider has a default-off feature. The `pmc` crate feature is
exposed as `service-pmc` by the facade and enables our existing JSON format.

## PMC PDF snapshots

`pmc::PmcAccessor` reads the [PMC Article Datasets](https://pmc-oa-opendata.s3.amazonaws.com/README.txt)
from a supplied byte accessor rooted at `pmc-oa-opendata`, or a mirror preserving
its object paths. It follows the PDF-snapshot behavior of
[`dentiny/pub-medical-arrow-rs`](https://github.com/dentiny/pub-medical-arrow-rs)
without depending on `pmc-io` or `pmc-arrow`.

```rust,no_run
use open_arrow_access::{
    Accessor, Result,
    service::pmc::{PmcAccessor, PmcRequest},
    storage::FsAccessor,
};

async fn read() -> Result<()> {
    let source = PmcAccessor::new(FsAccessor::new("/data/pmc-mirror").await?);
    let mut reader = source.open(PmcRequest::new("metadata/PMC123.2.json")).await?;
    while let Some(batch) = reader.next().await? {
        // Each row contains one metadata object and its original PDF.
        assert_eq!(batch.num_columns(), 9);
    }
    Ok(())
}
```

Enable `service-pmc,storage-fs` for that example, or run:

```sh
cargo run --example read_pmc --features service-pmc,storage-fs -- /data/pmc-mirror metadata/PMC123.2.json
```

`PmcRequest::files([...])` reads explicit objects in caller order and retains
duplicates. `PmcRequest::prefix("metadata/")` streams recursive `.json` discovery
in backend order; it does not collect or sort all paths. For nonrecursive
listing, set `recursive = false` on the request's `PmcInput::Prefix(ListRequest)`.
An empty selection or a selection containing no PDFs emits no batches and
still exposes the fixed schema at open. `open` validates options and starts
listing if requested; metadata/PDF contents are fetched lazily by `next`.
Dropping the reader stops discovery and reading; there are no producer tasks.

For the public S3 bucket, use the existing generic OpenDAL storage adapter.
Depend directly on `open-arrow-access-storage` and the same OpenDAL version
with `services-s3` and `http-transport-reqwest` enabled, as described in the
[storage example](../README.md#quick-start):

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

The provider adds no S3 transport or second OpenDAL dependency. Byte options
are forwarded to both metadata and attachment opens. Byte layers, including
the existing decompression layers, can wrap the supplied accessor; apply them
explicitly to a compatible mirror, rather than assuming public PMC objects are
compressed. Limits apply to the bytes exposed by that accessor.

### Schema and mapping

`pmc::schema()` returns these columns, in order. Schema metadata contains
`pmc_arrow.row_kind = pdf_snapshot` for reference-schema compatibility.

| Column | Arrow type | Nullable | Mapping |
| --- | --- | --- | --- |
| `pmcid` | Utf8 | No | Canonicalize `pmcid`, e.g. ` pmc00123 ` → `PMC123` |
| `pmc_version` | UInt32 | Yes | Positive `version`; unknown remains null |
| `pmid` | Utf8 | Yes | String or unsigned integer PMID; integer becomes decimal text |
| `doi` | Utf8 | Yes | `doi` |
| `title` | LargeUtf8 | Yes | `title` |
| `license` | Utf8 | Yes | `license_code` |
| `source_uri` | Utf8 | No | Original `pdf_url`, including query/fragment |
| `pdf_bytes` | LargeBinary | No | Original, unmodified PDF bytes |
| `raw_metadata_json` | LargeUtf8 | Yes | Complete original JSON, including whitespace and unknown fields |

Metadata must be exactly one valid UTF-8 JSON object. Missing/null `pdf_url`
skips the article without reading a PDF. Other failures are terminal errors,
including malformed mapped fields and missing PDFs. Empty strings in optional
fields stay empty; duplicate JSON keys use the last value. Unknown fields,
including numbers outside the f64 range, remain in the raw JSON. The provider
normalizes these mapped fields into bounded JSONL batches and delegates their
Arrow conversion to `file_format::JsonAccessor`; only PDF bytes and raw JSON
use dedicated Arrow array builders.

PDF locators must refer to `s3://pmc-oa-opendata/`. Query/fragment components are
retained in provenance and removed for object lookup. Object keys are
percent-decoded once (`+` is literal); rooted, dot-component, control-character,
backslash and ambiguous separator paths are rejected. A mirror must preserve
the decoded bucket-relative paths. PDF validation checks `%PDF-x.y` followed
by CR/LF at byte zero; it does not parse the PDF or prove completeness.

### Options, limits and errors

`PmcRequest` implements `ReadOptionsProvider`. Projection addresses the final
nine-column schema, keeps caller field order and schema metadata, and preserves
row counts for an empty projection. It is validated before listing or reads.
Projection currently runs after complete article validation: omitting
`pdf_bytes` still fetches and validates PDFs.

The common nonzero batch row target is an upper bound. `PmcLimits` adds hard
per-object limits (4 MiB metadata, 256 MiB PDF by default) and a soft 64 MiB
batch payload target. The payload count includes PDF, raw JSON and normalized
metadata bytes, checked after each appended row; it may exceed the target by
one bounded article. This is not an RSS limit: upstream chunks, decoder state,
temporary objects and Arrow conversion allocations also consume memory.
Limits are checked from known lengths before reading and incrementally for
unknown or inaccurate lengths; exceeding them is a terminal decode error.

Files and records remain sequential and ordered. Batches may be smaller than
the row target. A failure discards the current unreturned batch, terminates the
reader, and retains underlying I/O error categories and source chains. Previously
returned batches remain delivered; no file is transparently replayed. Discovery
order is not a stable snapshot, and metadata/PDF reads are not atomic. The MD5
query parameter is preserved as provenance but is not verified here.

XML/text/media retrieval, OCR, extraction, source inventory snapshots, parallel
scans, indexes and output writers are separate capabilities. The
[source-attribute proposal](../docs/source-attributes.md) remains a future
planning extension.
