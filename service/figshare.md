# Figshare

## Service and data

Figshare publishes research articles with attached datasets, documents and other
research files. This provider resolves public article versions and lists articles
in a collection. File contents remain in their published formats.

Enable facade feature `service-figshare`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `article(id: NonZeroU64, version: Option<NonZeroU32>)` | `/v2/articles/{id}` or `/v2/articles/{id}/versions/{version}` | `RemoteFiles<ApiDocument>` |
| `collection_articles(id, page: u32, page_size: u16)` | `/v2/collections/{id}/articles?page=…&page_size=…` | One `ApiDocument` array page |

Article metadata contains `id`, `version`, license information and
`files[]` entries with `name` and `download_url`. `metadata().value()` retains the
parsed response. Empty-root listing yields original file names in response order;
`open(name)` returns a sequential `ByteReader`. Resolution does not download files.
`Some(version)` selects an explicit version; `None` selects the current article.
Embargoed articles fail. Public requests need no API key; pagination is explicit.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "id": 21422193,
  "version": 4,
  "license": {
    "name": "example-license"
  },
  "files": [
    {
      "name": "ocr.txt.gz",
      "download_url": "https://files.example.org/ocr.txt.gz"
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::figshare::FigshareService};

async fn inspect_article(id: std::num::NonZeroU64, version: std::num::NonZeroU32) -> Result<()> {
    let files = FigshareService::new()?.article(id, Some(version)).await?;
    println!("license: {}", files.metadata().value()["license"]);
    let mut names = files.list("".into()).await?;
    if let Some(name) = names.try_next().await? {
        println!("file: {}", name.locator());
        let mut reader = files.open(name.into()).await?;
        if let Some(chunk) = reader.next().await? {
            println!("first chunk: {} bytes", chunk.len());
        }
    }
    Ok(())
}
```

The example prints the article's license metadata, an available file name and
the size of its first original byte chunk. It does not decode the file into Arrow.

Source: [Figshare API](https://docs.figshare.com/v2/).
