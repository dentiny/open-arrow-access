# Regulations.gov

## Service and data

Regulations.gov hosts federal rulemaking documents, public comments and dockets.
This provider searches their JSON metadata and resolves advertised document and
attachment representations.

Enable facade feature `service-regulations`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `search(resource, filters, page: u8, page_size: u16)` | `/v4/{resource}` with filters and `page[number]`, `page[size]` | `ApiDocument` with `data`/`meta` |
| `record(resource, id)` | `/v4/{resource}/{id}`; documents/comments add `include=attachments` | `RemoteFiles<ApiDocument>` |

`RegulationsResource` selects Documents, Comments or Dockets. Configure an
explicit api.data.gov key with `new(key)`. Page numbers are 1–20 and sizes 5–250.
Inspect returned pagination metadata and split larger queries into date windows.
Record metadata retains `data` and `included`. Logical file names are
`document/file-000` or `attachment-000/file-000`, based on `fileFormats[].fileUrl`.
Records without available files retain their metadata. Readers return sequential
original bytes; search pages do not automatically download attachments.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "data": {
    "id": "EXAMPLE-1",
    "type": "documents",
    "attributes": {
      "fileFormats": []
    }
  },
  "included": [
    {
      "id": "EXAMPLE-ATT-1",
      "type": "attachments",
      "attributes": {
        "fileFormats": [
          {
            "fileUrl": "https://files.example.org/attachment.pdf"
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
use open_arrow_access::{
    Accessor, ByteAccessor, Result,
    service::regulations::{RegulationsResource, RegulationsService},
};

async fn inspect_record(key: &str, id: &str) -> Result<()> {
    let files = RegulationsService::new(key)?
        .record(RegulationsResource::Documents, id)
        .await?;
    println!("record: {}", files.metadata().value()["data"]);
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

The example prints one document record and reads the first chunk of an
available representation. Withdrawal/restriction metadata remains available for
caller policy; no joins or full-text extraction are performed.

Source: [Regulations.gov API](https://open.gsa.gov/api/regulationsgov/).
