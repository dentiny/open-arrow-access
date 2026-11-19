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
| `pmid` | UInt64 | Yes |
| `mid`, `doi`, `license_code`, `xml_url`, `pdf_url`, `text_url` | Utf8 | Yes |
| `title`, `citation` | LargeUtf8 | Yes |
| `is_pmc_openaccess`, `is_manuscript`, `is_historical_ocr`, `is_retracted` | Boolean | Yes |
| `media_urls` | List of nullable Utf8 items; JSON items must be strings | Yes |
| `content_url` | Utf8; selected PDF/XML/TXT URL, default PDF | Yes |

The Arrow `text_url` column contains the TXT URL from the input JSON's
`text_url` field, not inline article text. Like `pdf_url` and `xml_url`, its name
matches the source field. URL encoding and query parameters are preserved.
Use `text_url` in Arrow projections; no `text` alias is provided.

All 16 upstream metadata fields are retained once as typed top-level columns,
using their original names. Optional missing/null fields become null; duplicate
JSON keys use their final value. No duplicate `metadata` struct or JSON string
column is emitted. `content_url` is the only derived convenience column.

Unexpected upstream fields are rejected, including when projected away. The
schema must be extended for a new field before accepting it; fields are never
silently dropped. Known fields retain their existing type/nullability checks.
`ReadOptions` controls top-level projection/order; zero columns still yield one
row. Record metadata has `source = Some(request.object)`; schema metadata contains
`pmc.row_kind = article_metadata`. Content URLs remain strings and are not fetched.
Configure filesystem/S3 credentials and optional decompression on the input storage.

The metadata reader reuses one clean Arrow JSON decoder per thread. The input
schema is fixed; format selection and output projection are applied independently
for each document. Decoder state is discarded on errors and after documents
larger than 64 KiB, so a large article cannot permanently retain its parser
allocations in the thread cache. Output remains one independently owned Arrow row
per document; strict input validation still runs before projection.

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
one article without listing. Directory locators must end in `/`; ambiguous
prefixes such as `metadata` return `InvalidInput` before listing, so they cannot
also select `metadata_old/`. It streams recursive discovery in storage order,
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

## Select one content format

`PmcFormat` accepts `pdf`, `xml` and `txt` (`FromStr`/`Display`); PDF is the default.
The nullable `content_url` output column selects the chosen upstream URL:

| Option | `content_url` comes from |
| --- | --- |
| `PmcFormat::Pdf` / `pdf` (default) | `pdf_url` |
| `PmcFormat::Xml` / `xml` | `xml_url` |
| `PmcFormat::Txt` / `txt` | `text_url` |

For a directory or exact JSON object:

```rust,ignore
let source = PmcDatasetAccessor::new(storage).with_format(PmcFormat::Xml);
let rows = source.open(ReadRequest::new("metadata/")).await?;
```

A single-document accessor can use
`PmcAccessor::new(storage, PmcDecoder::new(PmcFormat::Xml))`; the default decoder
selects PDF. The full schema is the same across formats and supports projection,
including selecting only `content_url` or zero columns.

Selection does not fetch or parse the content. A downstream sink should ingest
**`content_url`** to obtain just the chosen representation. Original URL columns
and all original metadata fields remain available, including all three
content URLs when supplied. A missing selected URL stays null; no other format
is substituted. Format selection does not create additional article rows or
automatically download multiple representations.

## One task per inventory shard

Use a fixed inventory manifest to discover task inputs, then read each exact
shard independently. Shard keys come from the manifest; do not invent a numeric
shard index or scan the entire `metadata/` prefix in every task.

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, ByteReadRequest, ReadOptions, Result,
    layers::ParallelReadLayer,
    service::{
        pmc::{PmcInventoryShardAccessor, PmcInventoryShardRequest},
        pmc_inventory::PmcInventoryAccessor,
    },
    storage::OpenDalAccessor,
};
use std::num::NonZeroUsize;

async fn convert_shards(manifest: &str) -> Result<()> {
    let storage = OpenDalAccessor::new(open_arrow_access::service::pmc::bucket_operator()?);
    let shards = PmcInventoryAccessor::new(storage.clone())
        .shards(ByteReadRequest::new(manifest)).await?;
    for shard in shards {
        // Submit one downstream task with (manifest, shard), and a distinct destination.
        let request = PmcInventoryShardRequest::new(manifest, shard)
            .with_options(ReadOptions::default().with_projection(["pmcid", "pmid", "pdf_url"]));
        let source = PmcInventoryShardAccessor::new(storage.clone())
            .layer(ParallelReadLayer::new(NonZeroUsize::new(4).unwrap(), NonZeroUsize::MIN));
        let mut rows = source.open(request).await?;
        while let Some(batch) = rows.next().await? {
            // Feed typed article rows into a sink; select pdf_url for blob ingestion.
            let _ = batch;
        }
    }
    Ok(())
}
```

The inventory accessor's `shards` method reads only the manifest. Each article
task verifies that its shard is listed in that manifest, reads only that gzip CSV,
and URL-decodes each validated metadata key once. Discovery is streaming with one
inventory batch buffered; duplicate rows remain duplicate article requests.
Output uses the same flat PMC schema, retaining all upstream metadata fields.
Inventory columns and the article projection are independent; even a
zero-column article scan validates Bucket/Key and preserves row counts.

Each metadata object is an independent native scan partition. `ParallelReadLayer`
limits concurrent article readers. Read/byte options propagate through the
manifest, CSV shard and metadata reads. Missing or invalid metadata fails the
stream; this reader neither fetches PDFs nor silently skips articles. The
selected inventory fixes membership, not immutable article contents: JSON objects
may change after the inventory was generated. Inventory reports expire upstream,
so retain the manifest and CSV objects if tasks need to be replayed later.
