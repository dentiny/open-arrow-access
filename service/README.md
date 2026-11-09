# Dataset services

## Service and data

This crate connects external data sources to Open Arrow Access. A provider can
return discovery metadata, a named set of original files, archive members, or
Arrow batches. The individual guide states exactly which representation it reads.
All service guides live directly in this `service/` directory; `src/` contains
implementation code.

| Source | Data available through this library | Guide |
| --- | --- | --- |
| Hugging Face | Dataset repository files and Parquet config/split exports | [Hugging Face](huggingface.md) |
| Zenodo | Attachments of a public published research record | [Zenodo](zenodo.md) |
| Dataverse | Original files of an explicit released research dataset version | [Dataverse](dataverse.md) |
| Internet Archive | Public item file membership and original files | [Internet Archive](internet-archive.md) |
| Figshare | Versioned research article files and collection pages | [Figshare](figshare.md) |
| PMC | One article-version metadata JSON as one Arrow row | [Article metadata](pmc.md) |
| PMC inventory | Manifest-selected inventory CSV shards as Arrow rows | [Inventory](pmc-inventory.md) |
| arXiv bulk | PDF/source TAR manifests and archive member bytes | [Bulk archives](arxiv.md) |
| arXiv metadata | Raw OAI-PMH article metadata XML pages | [Metadata](arxiv-metadata.md) |
| bioRxiv / medRxiv | Dated preprint metadata pages and JATS XML links | [Preprints](biorxiv.md) |
| MECA | Scholarly ZIP package manifests and selected members | [MECA](meca.md) |
| Semantic Scholar | Release-pinned bulk partitions and diff manifests | [Bulk datasets](semantic-scholar.md) |
| Project Gutenberg | Per-book RDF metadata as one Arrow row | [Gutenberg](gutenberg.md) |
| DOAB | Open-access book metadata via OAI-PMH | [DOAB](doab.md) |
| OAPEN | Book metadata via OAI-PMH and selected hosted book bytes | [OAPEN](oapen.md) |
| HathiTrust | Full/update Hathifiles catalogue exports | [Hathifiles](hathifiles.md) |
| OpenStax | Commit-pinned textbook source ZIP archives | [OpenStax](openstax.md) |
| BHL | Biodiversity literature metadata, item links and OCR export files | [BHL](bhl.md) |
| Common Crawl | Crawl/segment WARC membership and separately composed record reads | [Common Crawl](commoncrawl.md) |
| CourtListener | Dated bulk table exports as decompressed CSV bytes | [CourtListener](courtlistener.md) |
| Federal Register | Rule/notice metadata and advertised document representations | [Federal Register](federal-register.md) |
| GovInfo | Government bulk directories and package representations | [GovInfo](govinfo.md) |
| Library of Congress | Digital-collection metadata, resource files and OCR JSON | [Library of Congress](library-of-congress.md) |
| USPTO | Patent/trademark bulk product pages and original files | [USPTO](uspto.md) |
| Regulations.gov | Documents, comments, dockets and attachment representations | [Regulations.gov](regulations.md) |

Each guide follows the same order: what the source contains, request/response
formats, then a complete Rust usage example and an explanation of its result.

## Request and response

Enable a concrete provider with its facade feature, for example
`service-figshare`. On `open-arrow-access-service` directly, the corresponding
feature is `figshare`. Shared `service::common` utilities are available without a
provider feature. Credentials, when required, are described in the provider guide;
Hugging Face additionally supports its environment/cached-token settings.

| Response family | How to consume it | What it contains |
| --- | --- | --- |
| `ApiDocument` | `document.value()` | Parsed upstream JSON, including unknown fields |
| `RemoteFiles<M>` / `ResolvedFileSet<A, M>` | `metadata()`, `list("")`, `open(name)` | Provider metadata plus a fixed logical file namespace; file bodies are lazy |
| `ByteReader` | `next().await` | Original or explicitly decoded byte chunks; a chunk is not a record |
| `RecordReader` | `metadata().schema`, `next().await` | Arrow batches with the declared schema |
| `OaiPage` | `bytes`, `record_count`, `resumption_token` | Exact XML response and continuation information |
| Archive session | inspect entries, then open a member | Original member names/metadata and decoded member bytes |

All fallible calls use the library's `Result<T>`. Readers return `Some(item)`
until EOF (`None`); errors and EOF terminate them. Byte readers from public
`Connection` are sequential; OpenDAL-backed providers can support exact ranges.
ZIP/Parquet consumers need an appropriate ranged source, not just any HTTP reader.

JSON-retaining providers expose parsed responses through `ApiDocument`.
Zenodo, Dataverse and Internet Archive instead retain their documented typed
metadata subsets. Arrow schemas are provider-specific or come from the selected
file format; downloading a PDF/XML/ZIP does not automatically create Arrow rows.
See [response shapes](response-schemas.md) for a common reference.

Shared guides: [connections](provider-connection.md), [API metadata](api-document.md),
[named files](remote-files.md), [downloads](http-downloads.md),
[custom transport](http-transport.md), [validation helpers](provider-validation.md),
[OAI-PMH](oai-protocol.md), [OAI provider configuration](oai-provider.md), and
[development fixtures](provider-test-fixtures.md).

## Usage and results

### Example setup

Examples use the root facade and ordinary async Rust functions. Add the feature
listed in your provider guide. For the executable Figshare example below:

```toml
[dependencies]
open-arrow-access = { git = "https://github.com/dentiny/open-arrow-access", features = ["service-figshare"] }
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::figshare::FigshareService};
use std::num::NonZeroU64;

#[tokio::main]
async fn main() -> Result<()> {
    // BHL's public OCR export is published as this Figshare article.
    let files = FigshareService::new()?
        .article(NonZeroU64::new(21422193).unwrap(), None)
        .await?;
    println!("selected version: {}", files.metadata().value()["version"]);
    let mut names = files.list("".into()).await?;
    if let Some(name) = names.try_next().await? {
        println!("available file: {}", name.locator());
        let mut reader = files.open(name.into()).await?;
        if let Some(chunk) = reader.next().await? {
            println!("first original chunk: {} bytes", chunk.len());
        }
    }
    Ok(())
}
```

This resolves one public article, prints its observed version and one file name,
and reads at most one original byte chunk. `None` intentionally selects the
current version; use an explicit version for repeatable selection. The bytes
may still be compressed. The provider does not infer a format or extract text.

For examples taking `opendal::Operator`, configure a backend first. A local mirror
can use an additional dependency on the matching OpenDAL version:

```toml
opendal = { version = "=0.59.2", default-features = false, features = ["services-fs"] }
```

```rust
use open_arrow_access::storage::opendal;

fn mirror_operator(root: &str) -> opendal::Result<opendal::Operator> {
    opendal::Operator::new(opendal::services::Fs::default().root(root))
}
```

Pass that operator and a key relative to its root to the selected guide's
function. For S3/other backends, enable/configure the upstream OpenDAL backend
instead. [Repository examples](../examples/) show complete command-line readers.
