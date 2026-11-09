# OAPEN Library

## Service and data

OAPEN hosts open-access academic books and their metadata. This provider
harvests OAI-PMH metadata and can stream an explicitly selected OAPEN-hosted book
URL found in that metadata.

Enable facade feature `service-oapen`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET query at `https://library.oapen.org/oai/request` | Response |
| --- | --- | --- |
| `records(from, until, set)` | `verb=ListRecords&metadataPrefix=xoai` plus supplied optional selectors | `OaiPage` |
| `resume(token)` | `verb=ListRecords&resumptionToken=…` | Next `OaiPage` |

Selectors are `Option<&str>` and use provider-supported date/set values. Each
call returns `OaiPage { bytes: Bytes, resumption_token: Option<String>, record_count: usize }`.
`bytes` is the exact XML response, including record headers, metadata and deleted
headers. It is not a normalized JSON object or Arrow table. `reader()` exposes
those same bytes through a sequential reader. Public harvesting needs no key;
continue until `resumption_token` is None and apply pacing between requests.

`open_book(url, ByteReadOptions)` returns a sequential `ByteReader` for an
OAPEN-hosted URL selected from harvested metadata. Publisher-hosted external URLs
are rejected; no publisher crawl or automatic format preference is performed.

## Usage and results

```rust
use open_arrow_access::{Result, service::oapen::OapenService};

async fn harvest_one_window(from: &str, until: &str) -> Result<()> {
    let service = OapenService::new()?;
    let mut page = service.records(Some(from), Some(until), None).await?;
    loop {
        println!(
            "records: {}, XML bytes: {}",
            page.record_count,
            page.bytes.len()
        );
        // Consume page.bytes here; keep deleted record headers if applying updates.
        let Some(token) = page.resumption_token.take() else {
            break;
        };
        // Apply your request pacing before requesting the next page.
        page = service.resume(&token).await?;
    }
    Ok(())
}

async fn read_hosted_book(url: &str) -> Result<()> {
    let mut reader = OapenService::new()?
        .open_book(url, Default::default())
        .await?;
    if let Some(chunk) = reader.next().await? {
        println!("first book chunk: {} bytes", chunk.len());
    }
    Ok(())
}
```

The example prints the record count and XML byte length for every page in a
selected window. The original XML is available for your parser; harvesting alone
does not produce article/book content or Arrow batches.

Sources: [provider metadata documentation](https://oapen.org/article/metadata), [shared OAI-PMH guide](oai-protocol.md).
