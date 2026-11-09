# Dataverse

## Service and data

Dataverse installations publish versioned research datasets with labelled files
and optional directory labels. This provider reads an explicit public released
version from the installation you configure.

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
Restricted files or tabular files without original length are unsupported.

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
