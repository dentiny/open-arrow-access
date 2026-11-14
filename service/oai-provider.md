# Configured OAI-PMH providers

## Service and data

`OaiProviderService<P>` shares the harvesting behavior of DOAB, OAPEN and arXiv.
A provider marker supplies origin, path and metadata prefix. This is a reusable
protocol wrapper, not an additional hosted dataset.

Enable facade feature `service-doab`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Implement `OaiProvider` with `ORIGIN`, `PATH` and `METADATA_PREFIX` constants on
a Clone + Debug + Send + Sync marker. The wrapper exposes `new`,
`with_connection`, `connection`, `record_format()`, `records(from, until, set)` and
`resume(token)`.
Responses are the same `OaiPage` XML/continuation/count values documented in the
[OAI-PMH guide](oai-protocol.md). Built-in aliases are documented in their own guides.

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

## Default Arrow records

`service.record_format()` returns the provider's default `XmlFormat`, ready for
`page.records(service.record_format())`. The built-in DOAB, OAPEN and arXiv metadata
providers share these protocol header columns:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `identifier` | non-null `Utf8` | Header identifier, retained as text. |
| `datestamp` | nullable `Utf8` | Original date/day or UTC timestamp text; null when absent. |
| `status` | nullable `Utf8` | Original header status, including `deleted`; null when absent. |
| `set_specs` | non-null `List<Utf8>` | Ordered set membership values, including duplicates; `[]` when absent. |

The format explicitly maps `/oai:OAI-PMH/oai:ListRecords/oai:record` using the OAI
2.0 namespace URI, so a default namespace or a different input prefix works.
Empty `ListRecords` and `noRecordsMatch` responses produce no rows with the same
schema as populated and deleted-only pages. Header-only/deleted records remain rows.
As with other XML formats, input without the standard namespace needs an explicit
record mapping.

```rust
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    service::doab::DoabService,
};

async fn read_headers() -> Result<()> {
    let service = DoabService::new()?;
    let page = service.records(None, None, None).await?;
    let records = page.records(service.record_format());
    let mut reader = records.open(ReadRequest::new("oai-page")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} headers", batch.num_rows());
    }
    // The same page still owns its exact XML and opaque continuation token.
    println!("next page: {:?}", page.resumption_token);
    Ok(())
}
```

This default selects protocol headers, not provider-specific metadata. The full
XML, including `metadata` and `about`, remains in `page.bytes`. To extract those
fields, continue passing your own `XmlFormat` to `page.records(format)`, or override
`OaiProvider::record_format()` on a custom marker. For example, start with the
built-in record path and replace its schema using
`Doab::record_format().with_schema(custom_xml_schema)` (import `OaiProvider`).
Explicit schemas supply their own namespace bindings. XML limits, projection,
batch sizes and scan planning use the existing XML format APIs; no automatic
pagination or extra network reads are introduced.
