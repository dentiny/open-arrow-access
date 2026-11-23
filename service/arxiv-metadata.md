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

## Typed arXivRaw records

`page.records(service.inferred_record_format())` uses the provider's fixed
[arXivRaw XSD](https://oaipmh.arxiv.org/OAI/arXivRaw.xsd) mapping instead of
sampling scalar types. Article IDs (including leading zeros and legacy IDs),
DOIs, dates, titles, authors and other schema-defined text remain `Utf8`.
The output contains `oai:header` and nullable `oai:metadata` structs; the latter
contains `arxiv:arXivRaw`. Header set memberships and raw version history are
always lists, including when only one value is present. Version entries retain
`@version`, `arxiv:date`, `arxiv:size` and `arxiv:source_type`. Schema-defined
optional fields are nullable. Deleted headers retain a null metadata struct;
empty pages expose the same full schema. The XML namespace URI, not the source
prefix spelling, selects each field.

This changes the former content-dependent inferred schema: arXiv field names
now use the stable `arxiv:` prefix rather than a sampled `ns0:` alias. It avoids
numeric-looking article IDs being converted to floats and late legacy IDs
failing decoding after the inference sample. All fields in the published XSD
are mapped, plus the optional schema-location attribute; newly introduced
upstream fields require a mapping update. Original page XML remains available.
`record_format()` still provides only the standard protocol-header columns.
Other OAI providers continue to use sampled inference unless they override the
new `OaiProvider::inferred_record_format` hook. Harvesting full metadata uses
`service.harvest().with_format(service.inferred_record_format())`; persisting
and reconciling a complete snapshot remains caller-owned.

### Full harvested record schema

Harvested records retain the complete OAI schema below, without the outer
content `metadata` wrapper. `?` means nullable. All scalar values are `Utf8`;
version list elements are non-null structs. The `oai:setSpec` list item is named
`oai:setSpec` and is nullable.

```text
oai:header: Struct
    ├── oai:identifier: Utf8?
    ├── oai:datestamp: Utf8?
    ├── @status: Utf8?
    └── oai:setSpec: List<Utf8?>?
oai:metadata: Struct?
    └── arxiv:arXivRaw: Struct?
        ├── arxiv:id: Utf8?
        ├── arxiv:submitter: Utf8?
        ├── arxiv:title: Utf8?
        ├── arxiv:authors: Utf8?
        ├── arxiv:categories: Utf8?
        ├── arxiv:comments: Utf8?
        ├── arxiv:proxy: Utf8?
        ├── arxiv:report-no: Utf8?
        ├── arxiv:acm-class: Utf8?
        ├── arxiv:msc-class: Utf8?
        ├── arxiv:journal-ref: Utf8?
        ├── arxiv:doi: Utf8?
        ├── arxiv:license: Utf8?
        ├── arxiv:abstract: Utf8?
        ├── @xsi:schemaLocation: Utf8?
        └── arxiv:version: List<item: Struct>?
            ├── @version: Utf8?
            ├── arxiv:date: Utf8?
            ├── arxiv:size: Utf8?
            └── arxiv:source_type: Utf8?
```

### Content conversion schema

PDF/LaTeX/XML `records().content(format)` uses a smaller
[conversion metadata schema](arxiv.md#exact-fields-of-the-content-metadata-struct).
The nine fields unavailable in the Cornell/Kaggle snapshot are absent from that
schema and from emitted Lance datasets, including when the source is OAI.
They are not emitted as NULL placeholders. Raw OAI harvesting above keeps them
so that incremental harvesting and deleted-record reconciliation remain possible.

Content metadata can come from lazy OAI `GetRecord` lookups, a reconciled
`ArxivMetadataIndex::from_oai_records(snapshot)`, or an indexed local/remote
`ArxivMetadataDatabase`. All three paths project to the same conversion schema.
See [automatic content metadata](arxiv.md#automatic-content-metadata).
