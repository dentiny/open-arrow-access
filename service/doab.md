# Directory of Open Access Books (DOAB)

## Service and data

DOAB is a directory of open-access scholarly books. This provider harvests book
metadata and links through DOAB's OAI-PMH endpoint; it does not download book text.

Enable facade feature `service-doab`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET query at `https://directory.doabooks.org/oai/request` | Response |
| --- | --- | --- |
| `records(from, until, set)` | `verb=ListRecords&metadataPrefix=xoai` plus supplied optional selectors | `OaiPage` |
| `resume(token)` | `verb=ListRecords&resumptionToken=…` | Next `OaiPage` |

Selectors are `Option<&str>` and use provider-supported date/set values. Each
call returns `OaiPage { bytes: Bytes, resumption_token: Option<String>, record_count: usize }`.
`bytes` is the exact XML response, including record headers, metadata and deleted
headers. It is not a normalized JSON object or Arrow table. `reader()` exposes
those same bytes through a sequential reader. Public harvesting needs no key;
continue until `resumption_token` is None and apply pacing between requests.

## Usage and results

```rust
use open_arrow_access::{Result, service::doab::DoabService};

async fn harvest_one_window(from: &str, until: &str) -> Result<()> {
    let service = DoabService::new()?;
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
```

The example prints the record count and XML byte length for every page in a
selected window. The original XML is available for your parser; harvesting alone
does not produce article/book content or Arrow batches.

Sources: [provider metadata documentation](https://www.doabooks.org/en/librarians/doab-metadata), [shared OAI-PMH guide](oai-protocol.md).
