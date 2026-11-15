# arXiv OAI-PMH metadata

## Service and data

arXiv hosts scholarly preprints. This entry point harvests raw article metadata
through OAI-PMH. PDF/source TAR downloads are described separately in the
[arXiv bulk guide](arxiv.md).

Enable facade feature `service-arxiv`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Upstream GET query at `https://oaipmh.arxiv.org/oai` | Response |
| --- | --- | --- |
| `records(from, until, set)` | `verb=ListRecords&metadataPrefix=arXivRaw` plus supplied optional selectors | `OaiPage` |
| `resume(token)` | `verb=ListRecords&resumptionToken=…` | Next `OaiPage` |

Selectors are `Option<&str>` and use provider-supported date/set values. Each
call returns `OaiPage { bytes: Bytes, resumption_token: Option<String>, record_count: usize }`.
`bytes` is the exact XML response, including record headers, metadata and deleted
headers. It is not a normalized JSON object or Arrow table. `reader()` exposes
those same bytes through a sequential reader. Public harvesting needs no key;
continue until `resumption_token` is None and apply pacing between requests.

## Usage and results

```rust
use open_arrow_access::{Result, service::arxiv_metadata::ArxivMetadataService};

async fn harvest_one_window(from: &str, until: &str) -> Result<()> {
    let service = ArxivMetadataService::new()?;
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
does not produce article/book content or Arrow batches. To read standard OAI
header columns with a fixed schema, including empty pages, use
`page.records(service.record_format())`; see the
[default Arrow record format](oai-provider.md#default-arrow-records).

Sources: [provider metadata documentation](https://info.arxiv.org/help/oa/index.html), [shared OAI-PMH guide](oai-protocol.md).
