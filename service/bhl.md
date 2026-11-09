# Biodiversity Heritage Library

## Service and data

The Biodiversity Heritage Library provides digitized biodiversity literature,
bibliographic metadata and item-level text/PDF/image links. It also publishes a
Smithsonian OCR export on Figshare.

Enable facade feature `service-bhl`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream request | Library response |
| --- | --- | --- |
| `bulk_export(version: Option<NonZeroU32>)` | Figshare article `21422193`, optional explicit version | `RemoteFiles<ApiDocument>`; no BHL key needed |
| `get(operation, parameters)` | `/api3?op=…&format=json&apikey=…` | `ApiDocument` with `Status` and `Result` |
| `item(id: NonZeroU64)` | `GetItemMetadata` for one item | `RemoteFiles<ApiDocument>` |

`BhlOperation` covers title/item/page/part metadata, collections and statistics.
Use `new(Some(key))` for API v3 calls or `new(None)` for the public bulk export.
Item files are named `text`, `pdf`, `images` when their URLs are available.
Bulk-export files retain Figshare filenames. Both paths expose sequential original
bytes and complete parsed metadata; they do not produce Arrow rows.

Illustrative upstream API item shape (values are examples;
additional fields may be present):

```json
{
  "Status": "ok",
  "Result": [
    {
      "ItemID": 42,
      "ItemTextUrl": "https://files.example.org/item.txt",
      "ItemPDFUrl": null
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::bhl::BhlService};

async fn inspect_ocr_export(version: std::num::NonZeroU32) -> Result<()> {
    let files = BhlService::new(None)?.bulk_export(Some(version)).await?;
    println!("export metadata: {}", files.metadata().value());
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

The example selects a fixed export version and prints its metadata and one
file's first chunk. To read a specific API item instead, construct with a BHL key
and call `item(id)`; optional missing PDF/text links do not remove the metadata.

Sources: [BHL API v3](https://www.biodiversitylibrary.org/docs/api3.html), [Figshare guide](figshare.md).
