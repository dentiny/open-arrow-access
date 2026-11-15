# Dataverse

## Service and data

Dataverse installations publish versioned research datasets with labelled files
and optional directory labels. This provider reads an explicit released version
from the installation you configure. Public datasets work anonymously; restricted files require a token
and permission granted by the installation.

Enable facade feature `service-dataverse`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`DataverseService::new(origin)?.dataset(persistent_id, version).await` sends
`GET /api/v1/datasets/:persistentId/versions/{version}` with `persistentId`,
`excludeFiles=false`, `excludeMetadataBlocks=true`. Identifiers support DOI and
Handle forms; versions are explicit strings such as `"1.0"`.

The upstream response has `status`, `data.datasetPersistentId`, version fields
and `data.files[]`. The library returns `DataverseAccessor` with typed metadata:
`persistent_id()`, `version()`, and `files(): &[DataverseFile { name, id, size }]`.
Names combine directory/label; original downloads use
`/api/access/datafile/{id}?format=original`. Trusted original lengths avoid HEAD.
Readers support sequential bytes and ranges. A `.tab` label can identify an
original pre-ingestion representation; it does not guarantee tab-delimited output.
Without credentials, restricted files are rejected during resolution. With
credentials, the server enforces file permissions when bytes are requested.
Tabular files without original length remain unsupported.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "status": "OK",
  "data": {
    "datasetPersistentId": "doi:10.0000/EXAMPLE",
    "versionNumber": 1,
    "versionMinorNumber": 0,
    "versionState": "RELEASED",
    "files": [
      {
        "label": "table.tab",
        "directoryLabel": "data",
        "restricted": false,
        "dataFile": {
          "id": 42,
          "filesize": 1024,
          "tabularData": false
        }
      }
    ]
  }
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::dataverse::DataverseService};

async fn inspect_dataset(origin: &str, persistent_id: &str, version: &str) -> Result<()> {
    let files = DataverseService::new(origin)?
        .dataset(persistent_id, version)
        .await?;
    println!("{} version {}", files.persistent_id(), files.version());
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

For an installation such as `https://dataverse.harvard.edu`, the example
prints the selected dataset/version and reads an original file chunk. It does
not silently select latest/draft versions or return all dataset metadata blocks.

Source: [Dataverse data access API](https://guides.dataverse.org/en/latest/api/dataaccess.html).

## Authentication

Use an installation-specific Dataverse API token via `with_token`:

```rust
# use open_arrow_access::{Result, service::dataverse::DataverseService};
# async fn authenticated(origin: &str, token: &str) -> Result<()> {
let files = DataverseService::new(origin)?
    .with_token(token)?
    .dataset("doi:10.7910/DVN/EXAMPLE", "1.0")
    .await?;
# Ok(())
# }
```

For installations configured for OIDC bearer authentication, use
`.with_bearer_token(token)?` instead. The last authentication setter replaces
any previous scheme. Empty or invalid HTTP header values are rejected locally.
Tokens are supplied explicitly; the provider does not read environment variables
or obtain/refresh tokens.

Metadata queries and lazy original downloads (including ranged reads) use the
same credentials. API tokens use `X-Dataverse-key`; bearer tokens use
`Authorization: Bearer ...`. Credentials never enter URLs or service debug
output. Headers survive same-origin redirects, and are omitted after any
cross-origin redirect, including a later redirect back to the installation.

Use `with_context` for a custom TLS/proxy or test transport. With authentication,
the supplied transport must return single-hop responses so the provider can
apply redirect credential rules. `with_http_config` continues to apply retries
and timeouts to both metadata and file requests.

Authentication does not add draft/latest version selection or preview-URL
resolution. Only explicit released versions are supported.

See [Dataverse API tokens and authentication](https://guides.dataverse.org/en/latest/api/auth.html).
