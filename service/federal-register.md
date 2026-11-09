# Federal Register

## Service and data

The Federal Register publishes U.S. federal agency rules, proposed rules and
notices. This provider searches document metadata and resolves the document's
advertised PDF, XML, text and HTML representations.

Enable facade feature `service-federal-register`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `search(&[(name, value)])` | `/api/v1/documents.json` with supplied query pairs | `ApiDocument` with `results` and pagination fields |
| `next_page(url)` | Returned `next_page_url`, same origin | Next `ApiDocument` page |
| `document(number)` | `/api/v1/documents/{number}.json` | `RemoteFiles<ApiDocument>` |

Document metadata retains `document_number` and every other parsed field. File
names are available URL field names: `pdf_url`, `full_text_xml_url`, `raw_text_url`,
`body_html_url`, `html_url`, `public_inspection_pdf_url`. Missing/null links produce
no file while retaining the metadata. File readers emit sequential original bytes.
Public access needs no key; callers advance search cursors themselves.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "document_number": "2026-12345",
  "pdf_url": "https://files.example.org/document.pdf",
  "full_text_xml_url": null
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Result, service::federal_register::FederalRegisterService,
};

async fn inspect_document(number: &str) -> Result<()> {
    let files = FederalRegisterService::new()?.document(number).await?;
    println!("document: {}", files.metadata().value()["document_number"]);
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

The output identifies the document and an available representation. The byte
chunk may be PDF, XML, plain text or HTML, according to the selected logical name.

Source: [Federal Register API](https://www.federalregister.gov/developers/documentation/api/v1).
