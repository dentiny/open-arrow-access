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
| WARC/segment accessor `open(request)` | Arrow reader from the caller-supplied WARC-to-Arrow accessor |

Manifest accessors must supply decoded path bytes, so gzip is explicit. The
record argument of `CommonCrawlWarcAccessor::new(manifests, format)` must return
Arrow batches for `open`; the native `file_format::warc::WarcAccessor` instead
returns physical WARC records and cannot directly satisfy that Arrow contract.
There is no built-in Common Crawl Arrow schema or automatic payload extraction.
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
or emit Arrow columns. Use a caller-defined record accessor for crawl-wide Arrow
reading; its first file determines the output schema.

Source: [Common Crawl data access](https://commoncrawl.org/get-started).

For crawl-wide Arrow batches, supply your application's WARC-to-Arrow accessor
(the native WARC parser above returns a different item type):

```rust
use open_arrow_access::{
    Accessor, AccessorExt, ReadRequest, RecordReader, Result,
    decompression::{DecompressionLayer, gzip::Gzip},
    service::{
        commoncrawl::create_http_accessor,
        warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest},
    },
};

async fn read_crawl<F>(crawl: &str, format: F) -> Result<()>
where
    F: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let manifests =
        create_http_accessor("https://data.commoncrawl.org")?.layer(DecompressionLayer::new(Gzip));
    let dataset = CommonCrawlWarcAccessor::new(manifests, format);
    let mut reader = dataset.open(CommonCrawlWarcRequest::new(crawl)?).await?;
    println!("caller-defined schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        println!("first crawl batch: {} rows", batch.num_rows());
    }
    Ok(())
}
```

Here `format` owns record-file storage/decompression and Arrow conversion.
`resolve(request)` can instead return selected paths without opening record files.
Use `CommonCrawlSegmentAccessor` and `CommonCrawlSegmentRequest` for one segment.
