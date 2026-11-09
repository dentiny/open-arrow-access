# Internet Archive

## Service and data

Internet Archive items group digital materials such as books, audio, video,
software and associated metadata/derivative files. This provider resolves one
public item's file membership and opens exact filenames.

Enable facade feature `service-internet-archive`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`InternetArchiveService::new()?.item(identifier).await` sends
`GET https://archive.org/metadata/{identifier}`. Upstream JSON includes
`metadata.identifier` and `files[]`. The library returns `InternetArchiveAccessor`
with `identifier()` and `files(): &[InternetArchiveFile]`, retaining `name`, optional
`size`, `format`, `source`, `md5`. This is a typed subset, not the full response.

Empty-root listing preserves API order and nested filenames. `open(name)` uses
`/download/{identifier}/{filename}` through OpenDAL, with sequential bytes and
range reads. Membership is fetched once and file contents remain lazy. Size/MD5
metadata is not checksum verification. Hidden/restricted items are not treated
as empty public items. Membership can change upstream; no snapshot is implied.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
{
  "metadata": {
    "identifier": "example-item"
  },
  "files": [
    {
      "name": "book/text.txt",
      "size": "1024",
      "format": "Text",
      "source": "original",
      "md5": "00000000000000000000000000000000"
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Result, service::internet_archive::InternetArchiveService,
};

async fn inspect_item(identifier: &str) -> Result<()> {
    let files = InternetArchiveService::new()?.item(identifier).await?;
    println!(
        "item: {}, files: {}",
        files.identifier(),
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

The example prints the item ID, its file count and one exact name, then reads
an original file chunk. It does not borrow restricted books, authenticate with
cookies or choose a preferred media representation.

Source: [Internet Archive metadata API](https://archive.org/developers/md-read.html).
