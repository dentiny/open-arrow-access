# GovInfo

## Service and data

GovInfo distributes U.S. Government publications and their metadata. This
provider reads public bulk directories and API collections/packages, with XML,
PDF, text, MODS and archive representations when the selected package offers them.

Enable facade feature `service-govinfo`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `bulk_directory(prefix)` | `www.govinfo.gov/bulkdata/json/{prefix}` | `RemoteFiles<ApiDocument>` for direct files; child folders remain in metadata |
| `collections()` | `api.govinfo.gov/collections` | `ApiDocument` |
| `packages(collection, start, end, offset_mark, page_size)` | `/collections/{collection}/{start}[/{end}]` plus `offsetMark` and `pageSize` | One `ApiDocument` page |
| `next_page(url)` | API `nextPage`, same origin | Next page |
| `package(id)` | `/packages/{id}/summary` | `RemoteFiles<ApiDocument>` |

Use `new(None)` for public bulk access or `new(Some(key))` for API query-key
configuration. Start package paging with `"*"`; page size is 1–1000. Package file
names are keys in the response's `download` map. Downloads are sequential bytes.
The service does not recursively walk bulk folders. A ZIP and its separately
published XML members are distinct representations of potentially overlapping data.

Illustrative upstream package summary shape (values are examples;
additional fields may be present):

```json
{
  "packageId": "EXAMPLE-1",
  "download": {
    "pdfLink": "https://files.example.org/package.pdf",
    "xmlLink": "https://files.example.org/package.xml"
  }
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::govinfo::GovInfoService};

async fn inspect_bulk_directory(prefix: &str) -> Result<()> {
    let files = GovInfoService::new(None)?.bulk_directory(prefix).await?;
    println!("directory metadata: {}", files.metadata().value());
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

For a prefix such as `FR/2026/01`, the example prints the directory JSON and
reads one direct file chunk. Child directories must be requested separately.

Sources: [GovInfo bulk data](https://github.com/usgpo/bulk-data), [GovInfo API](https://github.com/usgpo/api).
