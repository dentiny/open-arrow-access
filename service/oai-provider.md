# Configured OAI-PMH providers

## Service and data

`OaiProviderService<P>` shares the harvesting behavior of DOAB, OAPEN and arXiv.
A provider marker supplies origin, path and metadata prefix. This is a reusable
protocol wrapper, not an additional hosted dataset.

Enable facade feature `service-doab`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Implement `OaiProvider` with `ORIGIN`, `PATH` and `METADATA_PREFIX` constants on
a Clone + Debug + Send + Sync marker. The wrapper exposes `new`,
`with_connection`, `connection`, `records(from, until, set)` and `resume(token)`.
Responses are the same `OaiPage` XML/continuation/count values documented in the
[OAI-PMH guide](oai-protocol.md). Endpoint configuration adds no extra row schema
or record normalization. Built-in aliases are documented in their own guides.

## Usage and results

```rust
use open_arrow_access::{
    Result,
    service::oai::{OaiProvider, OaiProviderService},
};

#[derive(Clone, Debug)]
struct BookCatalog;
impl OaiProvider for BookCatalog {
    const ORIGIN: &'static str = "https://directory.doabooks.org";
    const PATH: &'static str = "oai/request";
    const METADATA_PREFIX: &'static str = "xoai";
}

async fn harvest_catalog() -> Result<()> {
    let page = OaiProviderService::<BookCatalog>::new()?
        .records(None, None, None)
        .await?;
    println!("harvested metadata records: {}", page.record_count);
    Ok(())
}
```

The example configures the DOAB endpoint explicitly and obtains one original
XML page. Ordinary callers should use [DoabService](doab.md); custom markers are
for another compatible endpoint or prefix.
