# USPTO Open Data Portal

## Service and data

The USPTO Open Data Portal distributes patent and trademark bulk products.
Products have release/file catalogues and original downloadable files, commonly
archives or XML. This provider exposes product discovery and one product file page.

Enable facade feature `service-uspto`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `search(parameters)` | `/api/v1/datasets/products/search` with query pairs | `ApiDocument` |
| `product(id, offset: u64, limit: u16)` | `/api/v1/datasets/products/{id}?includeFiles=true&offset=…&limit=…` | `RemoteFiles<ApiDocument>` |

Use `UsptoService::new(key)` with an explicit ODP API key; `limit` must be
nonzero. Responses retain `bulkDataProductBag`, product identity, totals and
release metadata. Files come from `productFileBag.fileDataBag[]`, mapping
`fileName` to `fileDownloadURI`. Empty-root listing yields exact names and
`open(name)` streams sequential original bytes. The caller advances offsets.
API keys are scoped to the API origin and are stripped on cross-origin redirects.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "bulkDataProductBag": [
    {
      "productIdentifier": "EXAMPLE",
      "productFileBag": {
        "fileDataBag": [
          {
            "fileName": "grant.zip",
            "fileDownloadURI": "https://files.example.org/grant.zip"
          }
        ]
      }
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::uspto::UsptoService};

async fn inspect_product(key: &str, product_id: &str) -> Result<()> {
    let files = UsptoService::new(key)?.product(product_id, 0, 100).await?;
    println!("product page: {}", files.metadata().value());
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

The example prints one product file-page response, an available filename and
its first original chunk. It does not expand ZIP files or convert patent XML.

Source: [USPTO ODP API access](https://data.uspto.gov/apis/getting-started).
