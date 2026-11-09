# HathiTrust Hathifiles

## Service and data

Hathifiles are HathiTrust's full and incremental bibliographic/volume catalogue
exports. They include catalogue attributes and access/rights information; these
exports are not the full text of the books.

Enable facade feature `service-hathifiles`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`HathifilesService::new()?.catalog().await` sends
`GET https://www.hathitrust.org/files/hathifiles/hathi_file_list.json` and returns
`RemoteFiles<ApiDocument>`.

The upstream response is an array of catalogue entries with `filename` and other
metadata such as size or timestamp. `metadata().value()` keeps the parsed array.
Empty-root listing yields every advertised full/update filename in order, with
no rights or date filtering. `open(name)` reads original sequential gzip bytes
from `/files/hathifiles/{filename}`. Add gzip and TSV decoding separately. The
listing is mutable; retain the selected name and metadata when recording a run.

Illustrative upstream response shape (values are examples;
additional fields may be present):

```json
[
  {
    "filename": "hathi_full_20261101.txt.gz",
    "full": true,
    "size": 1024,
    "created": "2026-11-01"
  }
]
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::hathifiles::HathifilesService};

async fn inspect_catalog() -> Result<()> {
    let files = HathifilesService::new()?.catalog().await?;
    println!("catalog: {}", files.metadata().value());
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

The example prints the catalogue and reads the first compressed chunk of one
export. The bytes are still gzip-compressed, not TSV rows or Arrow batches.

Source: [Hathifiles listing generator](https://github.com/hathitrust/hathifiles/blob/main/jobs/update_hathifile_listing.rb).
