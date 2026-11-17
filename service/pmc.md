# PMC article-version metadata

## Service and data

PubMed Central (PMC) distributes biomedical article metadata and links to
article content. This entry point converts one article-version metadata JSON
object to one Arrow row. `PmcDatasetAccessor` also discovers metadata files
for a release or subset. See also the [inventory reader](pmc-inventory.md).

Enable facade feature `service-pmc`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Construct `PmcAccessor::with_default_decoder(storage)` and open
`ReadRequest::new("metadata/PMC123.2.json")`. The locator belongs to your storage
root. Input JSON uses upstream field names, for example:

```json
{"pmcid":"PMC123","version":2,"title":"Example article","pdf_url":null}
```

The result is a `RecordReader` with exactly one row for a valid object. Full schema:

| Fields | Arrow type | Nullable |
| --- | --- | --- |
| `pmcid` | Utf8 | No |
| `version` | UInt32, positive when supplied | Yes |
| `pmid`, `doi`, `license_code`, `xml_url`, `pdf_url`, `text` | Utf8 | Yes |
| `title`, `citation` | LargeUtf8 | Yes |
| `is_pmc_openaccess`, `is_manuscript`, `is_historical_ocr`, `is_retracted` | Boolean | Yes |
| `media_urls` | List of nullable Utf8 items; JSON items must be strings | Yes |

The Arrow `text` column contains the URL from the input JSON's `text_url` field,
not inline article text. URL encoding and query parameters are preserved. The
input JSON field remains `text_url`; use `text` in Arrow projections.

Optional missing/null fields become null; unknown keys are ignored and duplicate
keys use their final value. Known fields are validated even when projected away.
`ReadOptions` controls top-level projection/order; zero columns still yield one
row. Record metadata has `source = Some(request.object)`; schema metadata contains
`pmc.row_kind = article_metadata`. Content URLs remain strings and are not fetched.
Configure filesystem/S3 credentials and optional decompression on the input storage.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    service::pmc::PmcAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read_article(operator: opendal::Operator, metadata_key: &str) -> Result<()> {
    let articles = PmcAccessor::with_default_decoder(OpenDalAccessor::new(operator));
    let mut reader = articles.open(ReadRequest::new(metadata_key)).await?;
    println!("schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        println!(
            "rows: {}, PMCID column: {:?}",
            batch.num_rows(),
            batch.column_by_name("pmcid")
        );
    }
    Ok(())
}
```

Given an operator rooted at your PMC mirror/bucket and an exact metadata key,
the example obtains one metadata row. A null `pdf_url` is a valid result; this
call does not download article full text, PDFs or media.

Source: [PMC Open Data distribution](https://pmc-oa-opendata.s3.amazonaws.com/README.txt).


## Release discovery and blob storage

`PmcDatasetAccessor` accepts the existing `ReadRequest`: `metadata/` selects
all metadata, a subdirectory selects a subset, and an exact `.json` key selects
one article without listing. It streams recursive discovery in storage order,
selecting lowercase `.json` files. Use metadata-only directories; a live listing
is not an atomic snapshot. Projection, byte options and scan planning are retained.

```rust,ignore
use open_arrow_access::{Accessor, ReadRequest, service::pmc, storage::OpenDalAccessor};

let operator = pmc::bucket_operator()?;
let source = pmc::PmcDatasetAccessor::new(OpenDalAccessor::new(operator.clone()));
let reader = source.open(ReadRequest::new("metadata/")).await?;

// Register the same bucket with open-arrow-sink's generic blob source API.
let storage = open_arrow_sink::storage::Storage::new(
    operator, pmc::BUCKET_URL.parse()?,
)?;
let downloader = downloader.with_source_storage(storage);
```

`bucket_operator()` uses anonymous S3 access in `us-east-1`, disables ambient
credential/config loading, and is rooted at `BUCKET_URL`. Callers may add their
own retry/timeout layers. `source.metadata_paths(&request)` exposes the same
streaming selection for inspectors. Existing `PmcAccessor` and `PmcDecoder`
remain available for single-document composition.
