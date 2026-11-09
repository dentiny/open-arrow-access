# Library of Congress

## Service and data

The Library of Congress API describes digital collections, including books,
newspapers, photographs, maps, manuscripts and audiovisual material. This
provider reads search/item metadata, resource file links and text-service OCR
responses; it does not represent the entire library catalogue.

Enable facade feature `service-library-of-congress`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `search(parameters, page, count)` | `www.loc.gov/search/?fo=json&sp=…&c=…` plus filters | Search `ApiDocument` |
| `next_page(url)` | Returned continuation, normalized to JSON/same origin | Next page |
| `item(id_or_url)` | `/item/{id}/?fo=json` | Item `ApiDocument` |
| `resource(id_or_url)` | `/resource/{id}/?fo=json` | `RemoteFiles<ApiDocument>` |
| `ocr(segment, format)` | `tile.loc.gov/text-services/word-coordinates-service` with `full_text=1` | OCR `ApiDocument` |

Search metadata includes `results`/pagination. Resources retain rights and all
parsed fields. Logical file names include `resource/pdf`, `resource/fulltext_file`
and `file-00000/representation-000/url` when those links exist. Expanded file
groups and their representations retain order; missing links are omitted.
File readers are sequential. Public API access needs no key. Follow response
pagination rather than assuming a single search page is complete.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "item": {
    "rights": [
      "example rights statement"
    ]
  },
  "resource": {
    "pdf": "https://files.example.org/book.pdf",
    "files": [
      [
        {
          "url": "https://files.example.org/page.tif",
          "mimetype": "image/tiff"
        }
      ]
    ]
  }
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Result, service::library_of_congress::LibraryOfCongressService,
};

async fn inspect_resource(resource_id: &str) -> Result<()> {
    let files = LibraryOfCongressService::new()?
        .resource(resource_id)
        .await?;
    println!("resource metadata: {}", files.metadata().value());
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

The example prints resource metadata and the name/first byte chunk of one
advertised representation. Use `ocr` separately when you want the parsed OCR
JSON response rather than downloading a linked file.

Sources: [LoC JSON API](https://www.loc.gov/apis/json-and-yaml/), [text services](https://www.loc.gov/apis/micro-services/text-services/).
