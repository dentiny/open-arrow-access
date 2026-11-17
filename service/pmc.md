# PMC article-version metadata

## Service and data

PubMed Central (PMC) distributes biomedical article metadata and links to
article content. `PmcAccessor` converts one article-version metadata JSON
object to one Arrow row. `PmcDatasetAccessor` discovers and reads metadata for
a whole release or a subset. The [inventory reader](pmc-inventory.md) is also
available for S3 inventory snapshots.

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

## Release and subset discovery

Use `PmcDatasetAccessor::public()` for anonymous access to the public bucket,
or `PmcDatasetAccessor::new(storage)` with a bucket-root mirror. Existing
single-object `PmcAccessor` / `PmcDecoder` composition remains available.

```rust
use open_arrow_access::{
    Accessor, Result,
    service::pmc::{PmcDatasetAccessor, PmcRequest},
};

async fn read_release() -> Result<()> {
    let source = PmcDatasetAccessor::public()?;
    let mut reader = source.open(PmcRequest::release()).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} metadata rows", batch.num_rows());
    }
    Ok(())
}
```

Choose the selection with:

- `PmcRequest::release()`: recursively list the current `metadata/` tree.
- `PmcRequest::prefix("metadata/subset/")`: recursively list a subtree or a
  versioned mirror prefix. Prefixes must end in `/`.
- `PmcRequest::files(["metadata/PMC123.1.json", "metadata/PMC456.2.json"])`:
  read an exact subset without listing, retaining caller order and duplicates.
- `PmcRequest::from_uri("pmc://oa")?`: the same release selection;
  `pmc://oa/<prefix>/` and `pmc://oa/<object>.json` select subsets.

The accessor streams discovery in storage listing order, filters for lowercase
`.json` objects, and reads each article lazily. Choose a metadata-only prefix;
arbitrary JSON files under that prefix are treated as article metadata.
A live public listing is not an atomic release snapshot. Pin a mirror prefix or
persist the explicit object list when reproducibility is required. Empty
selections return the projected schema with no rows. Listing failures and
metadata decoding failures propagate and terminate the scan.

`with_options` and `with_byte_options` retain projection and byte I/O settings;
`PmcRequest` implements `ReadOptionsProvider` for record layers. The dataset
accessor implements `ScanPlanner`, so `ParallelReadLayer` can read discovered
articles under its partition budget. `source.metadata_paths(&request).await?`
returns the same streaming selection for inspectors and custom pipelines.

See [read_pmc_release.rs](../examples/read_pmc_release.rs) for a runnable public
bucket example. Run it with a small subset URI when exploring.

## Registering article content with a sink

`service::pmc::{BUCKET_URL, bucket_operator}` provides a bucket-root OpenDAL
operator configured for `s3://pmc-oa-opendata/`, `us-east-1`, anonymous requests,
and disabled ambient credential/config loading. Source configuration stays
independent of the destination's credentials and region. Callers can apply
OpenDAL retry/timeout layers before use.

With compatible OpenDAL versions, register this operator through the sink's
existing generic source-storage API:

```rust,ignore
use open_arrow_access::service::pmc::{BUCKET_URL, bucket_operator};
use open_arrow_sink::storage::{BlobDownloader, Storage, Url};

let source = Storage::new(bucket_operator()?, Url::parse(BUCKET_URL)?)?;
let downloader = BlobDownloader::new(
    std::num::NonZeroUsize::new(8).unwrap(),
    std::num::NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
)?.with_source_storage(source);
// Install the downloader on the sink or the URL-to-blob layer in your pipeline.
```

This adds no compile-time dependency between access and sink. Keep PMC content
URLs intact in Arrow, including their `?md5=` query. Current sink versions strip
a single 32-hex-digit `md5` query when resolving S3 objects; this does not verify
the downloaded payload checksum. Metadata discovery itself does not download
PDFs, XML, text, or media.
