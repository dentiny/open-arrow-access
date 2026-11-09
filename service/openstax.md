# OpenStax book sources

## Service and data

OpenStax publishes educational textbooks. This provider accesses public book
source repositories in the OpenStax GitHub organization. A repository archive
contains its tracked source files, such as CNXML modules, collections and media;
it is not a rendered textbook row schema.

Enable facade feature `service-openstax`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

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
