# Common Crawl WARC data

## Service and data

Common Crawl publishes web crawl data, including WARC files containing crawl
records and gzip path inventories. This provider identifies a crawl/segment and
resolves its WARC membership. Record decoding is a separate composition step.

Enable facade feature `service-warc`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Request | Response |
| --- | --- |
| `CommonCrawl::new("CC-MAIN-YYYY-NN")` / `from_url(crawl_url)` | Validated crawl ID/origin |
| `CommonCrawlWarcRequest::new(crawl)` | Crawl-wide selector; inventory is `crawl-data/{crawl}/warc.paths.gz` |
| `CommonCrawlSegmentRequest::new(crawl, segment)` | Exact segment selector over the same inventory |
| WARC/segment accessor `resolve(request)` | `FileSetRequest { objects, options, byte_options }` in inventory order |
| WARC/segment accessor `open(request)` | Arrow reader from the native WARC view or a caller-supplied record accessor |

Manifest accessors must supply decoded path bytes, so gzip is explicit. The
record argument of `CommonCrawlWarcAccessor::new(manifests, format)` must return
Arrow batches for `open`; the native `file_format::warc::WarcAccessor` instead
returns physical WARC records and cannot directly satisfy that Arrow contract.
Use `CommonCrawlWarcAccessor::from_warc_bytes(manifests, bytes)` or
`CommonCrawlSegmentAccessor::from_warc_bytes(manifests, bytes)` for the built-in
[physical-record Arrow schema](../docs/formats.md#warc-parser). Projection uses
`request.with_options(ReadOptions { projection: Some(columns), ..Default::default() })`.
Automatic HTTP payload extraction and HTML-to-text conversion are not provided.
For one selected file, the native parser can be used directly as below.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, AccessorExt, ByteReadRequest, Result,
    decompression::{DecompressionLayer, gzip::Gzip},
    file_format::warc::WarcAccessor,
    service::commoncrawl::{CommonCrawl, create_http_accessor},
};

async fn read_selected_warc(crawl: &str, object: &str) -> Result<()> {
    let source = CommonCrawl::new(crawl)?;
    let bytes = create_http_accessor(source.origin())?.layer(DecompressionLayer::new(Gzip));
    let warc = WarcAccessor::new(bytes);
    let mut reader = warc.open(ByteReadRequest::new(object)).await?;
    if let Some(record) = reader.next().await? {
        println!("WARC record body: {} bytes", record.body().len());
    }
    Ok(())
}
```

Also enable `decompression-gzip` and `format-warc`. Supply a selected
`crawl-data/.../segments/.../warc/...warc.gz` object from the inventory. The
example obtains one physical WARC record's body; it does not turn HTML into text
or emit Arrow columns. The Arrow variant below uses the same decoded byte source
and selects columns from the fixed physical-record schema.

Source: [Common Crawl data access](https://commoncrawl.org/get-started).

For crawl-wide Arrow batches with selected columns, use the built-in Arrow view:

```rust
use open_arrow_access::{
    Accessor, AccessorExt, ReadOptions, Result,
    decompression::{DecompressionLayer, gzip::Gzip},
    service::{
        commoncrawl::create_http_accessor,
        warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest},
    },
};

async fn read_crawl(crawl: &str) -> Result<()> {
    let manifests =
        create_http_accessor("https://data.commoncrawl.org")?.layer(DecompressionLayer::new(Gzip));
    let bytes =
        create_http_accessor("https://data.commoncrawl.org")?.layer(DecompressionLayer::new(Gzip));
    let dataset = CommonCrawlWarcAccessor::from_warc_bytes(manifests, bytes);
    let request = CommonCrawlWarcRequest::new(crawl)?.with_options(ReadOptions {
        projection: Some(vec!["record_id".into(), "target_uri".into()]),
        ..Default::default()
    });
    let mut reader = dataset.open(request).await?;
    println!("projected schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        println!("first crawl batch: {} rows", batch.num_rows());
    }
    Ok(())
}
```

Manifest and WARC gzip decoding are configured separately. Projection omits Arrow
arrays for unselected fields; complete WARC bodies are still read and validated.
Use `CommonCrawlWarcAccessor::new(manifests, format)` for a custom record schema.
`resolve(request)` can instead return selected paths without opening record files.
Use `CommonCrawlSegmentAccessor` and `CommonCrawlSegmentRequest` for one segment.
