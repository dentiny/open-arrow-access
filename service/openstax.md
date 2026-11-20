# OpenStax book sources

## Service and data

OpenStax publishes educational textbooks. This provider accesses public book
source repositories in the OpenStax GitHub organization. A repository archive
contains its tracked source files, such as CNXML modules, collections and media;
it is not a rendered textbook row schema.

Enable facade feature `service-openstax`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## File hierarchy and dataset boundaries

The following paths are concrete examples or explicitly marked templates, not
an exhaustive live inventory. This section describes the selected upstream
representation; it does not add automatic corpus discovery or parsing.

```text
openstax/osbooks-physics @ <commit-sha>/
  collections/physics.collection.xml  # ordered collection spine
  modules/m54082/index.cnxml           # section source
  media/CNX_HSPhysics_01_00_galaxy.jpg  # referenced asset
  META-INF/
  LICENSE
<repository>.zip                       # transport snapshot, not a module shard
```

The Physics collection nests `col:subcollection` elements and references modules with
`col:module document="m54082"`. That module contains a relative image reference into
`../../media/`. Resolve paths relative to each source file. The example establishes a
real repository layout, not a promise that all OpenStax repositories contain one book or
identical directory names. Filter organization repository listings deliberately; they
include software repositories.

## Sharding strategy and current library boundary

`OpenStaxService::book(repository, revision)` resolves the revision to a commit and returns one logical `{repository}.zip` through `RemoteFiles<BookMetadata>`. The response streams archive bytes; it does not emit CNXML module rows or expand archives. Stage bytes in caller-owned ranged storage before `ZipArchive` access. Book/module planning below is an application proposal; HTTP byte chunks are not module boundaries.

| Boundary | Meaning |
| --- | --- |
| Acquisition | One repository archive pinned to a resolved commit SHA. |
| Logical grouping | Collection/book, ordered module occurrences and referenced assets. |
| Proposed shard | Repository + commit + collection, optionally a group of module occurrences. |
| Unsafe boundary | ZIP response chunks, lexical module filename order or arbitrary CNXML byte ranges. |

Recommended application plan (not a new provider request option):

Start with one repository/commit per acquisition task. Parse collection order before
planning smaller work units. Identify a module occurrence by repository, commit,
collection path and its position in the collection spine; module ID alone does not
encode reading order or distinguish reuse. Group whole module occurrences by measured
source-plus-asset bytes. Persist a module/asset manifest and keep shared media
content-addressed or explicitly referenced, so packaging it twice does not create two
textual sections. Retain XML, mathematics, cross-references, attribution and original
assets even when a derived text view is produced.

Pin a manifest containing exact source keys/URLs, logical identities, source
version/date, representation, byte sizes and downloaded-byte digests, parser/schema
version and exact shard membership. Keep unknown metadata and original bytes
where a typed projection cannot preserve them. Archive transport chunks, Arrow
batches and output fragments do not define upstream dataset shards.

For independent record files, compose an explicit record decoder/schema with
file-set planning and the [parallel execution layer](../docs/parallel-read.md).
A file set defaults to one partition per file; native file planning is an
additional opt-in when the inner format supports it. Independent compressed files
can use separate decoder state. This does not imply parallel decompression or
range-readable records within a single compressed stream. Bound active readers,
parser buffers and queued output; parallelism is not a process memory cap.
Apply one compatible schema per scan and preserve source identities independently
of batch arrival order.

## Snapshot refresh and validation

Resolve a branch or tag once and record the resulting SHA. A retry uses the same SHA and
archive digest. A new commit is a new snapshot; compare collection, module and asset
digests, including dependencies of unchanged module text. Check each selected book's
license and attribution metadata rather than assigning one license to the organization.

Verify every collection reference resolves, spine order survives reconstruction, and
assets exist. Count module occurrences and unique modules separately. Check that all
requested repositories/collections are represented and that archive extraction handles
its actual root directory safely.

These are design recommendations based on the existing provider source and
publisher references. No live corpus conversion, full inventory acquisition or
sharding performance measurement was performed for this documentation.

- [Physics repository](https://github.com/openstax/osbooks-physics)
- [Physics collection](https://github.com/openstax/osbooks-physics/blob/main/collections/physics.collection.xml)
- [Module and image reference](https://github.com/openstax/osbooks-physics/blob/main/modules/m54082/index.cnxml)

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `repositories(page, per_page)` | `api.github.com/orgs/openstax/repos?type=public&…` | One unfiltered `ApiDocument` array page |
| `book(repository, revision)` | `/repos/openstax/{repo}/commits/{revision}`, then a lazy codeload `/openstax/{repo}/zip/{sha}` URL | `RemoteFiles<BookMetadata>` |

`BookMetadata` has `repository`, `commit_sha`, `commit: ApiDocument`. The single
logical file is `{repository}.zip`. The branch/tag is resolved once; its download
uses the resolved SHA. ZIP bytes are sequential, so caller-owned ranged storage
is needed before using `ZipArchive`. Submodules/LFS pointers are kept as GitHub
supplies them. Public requests need no key; `with_connections` accepts explicit
API/archive connections when custom authentication is needed.

Illustrative upstream GitHub commit shape (values are examples;
additional fields may be present):

```json
{
  "sha": "0123456789012345678901234567890123456789",
  "commit": {
    "message": "Example snapshot"
  }
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::openstax::OpenStaxService};

async fn inspect_book(repository: &str, revision: &str) -> Result<()> {
    let files = OpenStaxService::new()?.book(repository, revision).await?;
    println!("commit: {}", files.metadata().commit_sha);
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

With a repository such as `osbooks-physics`, the example prints its pinned
commit, the ZIP name and the size of its first compressed chunk. It does not
expand the archive or fetch submodule repositories.

Source: [OpenStax GitHub organization](https://github.com/openstax).
