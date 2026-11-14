# OAI-PMH ListRecords protocol

## Service and data

OAI-PMH is a metadata-harvesting protocol used here by DOAB, OAPEN and arXiv.
`OaiService` supplies the shared ListRecords paging implementation. It harvests
XML metadata records and deleted headers, not full-text payloads.

Enable facade feature `service-doab`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`OaiService::new(connection, relative_path, metadata_prefix)` configures an
endpoint. `records(from, until, set)` sends `verb=ListRecords`, `metadataPrefix`
and supplied optional selectors. `resume(token)` sends only `verb=ListRecords`
and `resumptionToken`, as required for continuation.

Both return `OaiPage { bytes, resumption_token, record_count }`. `bytes` preserves
the original XML envelope, header/metadata elements and deleted records;
`reader()` returns those bytes as a sequential reader. Empty tokens end paging.
The caller controls date windows, sets and pacing. Choose the provider alias
when its standard endpoint/prefix is sufficient.

An illustrative XML page (the contents of `metadata` are provider-specific):

```xml
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2026-11-01T00:00:00Z</responseDate>
  <request verb="ListRecords" metadataPrefix="xoai">https://example.org/oai</request>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:example.org:book/42</identifier>
        <datestamp>2026-11-01</datestamp>
      </header>
      <metadata><!-- Provider-specific book/article metadata --></metadata>
    </record>
    <resumptionToken>next-page-token</resumptionToken>
  </ListRecords>
</OAI-PMH>
```

## Usage and results

```rust
use open_arrow_access::{
    Result,
    service::{common::Connection, oai::OaiService},
};

async fn read_initial_page() -> Result<()> {
    let protocol = OaiService::new(
        Connection::new("https://directory.doabooks.org")?,
        "oai/request",
        "xoai",
    )?;
    let page = protocol.records(None, None, None).await?;
    println!(
        "records: {}, bytes: {}, continuation: {:?}",
        page.record_count,
        page.bytes.len(),
        page.resumption_token
    );
    Ok(())
}
```

The example obtains exactly one DOAB XML page. Enable the service crate's
`oai` feature directly when using it without a facade provider feature; facade
features such as `service-doab` already expose this module. Follow the returned
token for additional pages. Configured providers also offer a
[default Arrow record format](oai-provider.md#default-arrow-records) for standard
header columns, preserving a stable schema for empty pages.

Source: [OAI-PMH specification](https://www.openarchives.org/OAI/openarchivesprotocol.html).
