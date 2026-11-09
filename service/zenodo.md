# Zenodo

## Service and data

Zenodo is a research repository containing datasets, publications, software and
other research outputs. This provider resolves the attachments of one public,
published record version.

Enable facade feature `service-zenodo`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`ZenodoService::new()?.record(id: NonZeroU64).await` sends
`GET https://zenodo.org/api/records/{id}`. Use a record-version ID, not a concept
DOI. `with_endpoint(origin)` selects a sandbox or mirror; no credentials are
loaded implicitly.

The upstream JSON contains `id` and `files[]` with `key`, `size`, `checksum`.
The library returns `ZenodoAccessor = ResolvedFileSet<OpenDalAccessor, ZenodoMetadata>`;
`id()` and `files()` expose that typed subset, not the entire upstream JSON.
Empty-root listing yields exact keys; `open(key)` uses the canonical
`/api/records/{id}/files/{key}/content` route. File readers provide sequential
bytes and exact ranges through OpenDAL. Resolution/listing fetch no attachments.
Checksums are exposed but not verified; an empty file list is valid.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "id": 123,
  "files": [
    {
      "key": "data.csv",
      "size": 1024,
      "checksum": "md5:00000000000000000000000000000000"
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::zenodo::ZenodoService};

async fn inspect_record(id: std::num::NonZeroU64) -> Result<()> {
    let files = ZenodoService::new()?.record(id).await?;
    println!(
        "record: {}, attachments: {}",
        files.id(),
        files.files().len()
    );
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

The example prints the resolved ID and attachment count, then reads one
attachment's first original byte chunk. File formats are selected by the record;
apply the appropriate format/decompression reader for Arrow output.

Source: [Zenodo Records API](https://developers.zenodo.org/#records).
