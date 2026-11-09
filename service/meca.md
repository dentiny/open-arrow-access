# MECA scholarly transfer archives

## Service and data

MECA is an archive/manifest convention for transferring scholarly article
packages. A bundle can contain JATS XML, PDFs and supplements. This module is an
archive reader, not a hosted storage service or a paper-discovery API.

Enable facade feature `service-meca`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`MecaArchive::open(input: Box<dyn ByteReader>, ZipLimits).await` requires a
known-length, range-capable ZIP reader from your storage. It initially reads the
ZIP directory and the transfer manifest.

| Response method | Value |
| --- | --- |
| `manifest()` | Exact manifest XML `Bytes` |
| `files()` | Ordered `MecaFile` instances: `item_id`, optional `item_type`, `href`, optional `media_type`, resolved `entry_index` |
| `archive()` | Underlying `ZipArchive`, including members not listed in the manifest |
| `archive().open_entry(index, chunk_size)` | Sequential decoded bytes of a selected member |

Relative URI references are resolved against ZIP members; traversal, missing and
ambiguous references fail. External DTDs are not fetched. This validates archive
membership/reference handling rather than every MECA schema rule.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ByteReadRequest, Result,
    decompression::zip::ZipLimits,
    service::meca::MecaArchive,
    storage::{OpenDalAccessor, opendal},
};
use std::num::NonZeroUsize;

async fn read_bundle(operator: opendal::Operator, bundle: &str) -> Result<()> {
    let input = OpenDalAccessor::new(operator)
        .open(ByteReadRequest::new(bundle))
        .await?;
    let package = MecaArchive::open(input, ZipLimits::default()).await?;
    println!(
        "manifest: {} bytes, {} file references",
        package.manifest().len(),
        package.files().len()
    );
    if let Some(file) = package.files().first() {
        println!("item: {}, href: {}", file.item_id, file.href);
        let mut reader = package
            .archive()
            .open_entry(file.entry_index, NonZeroUsize::new(65536).unwrap())
            .await?;
        if let Some(chunk) = reader.next().await? {
            println!("decoded member chunk: {} bytes", chunk.len());
        }
    }
    Ok(())
}
```

Also enable facade feature `decompression-zip` for the public `ZipLimits` path.
The example obtains manifest metadata and decoded bytes of a listed member.
A sequential HTTP ZIP download must first be materialized in caller-owned
ranged storage; passing it directly to `MecaArchive::open` is unsupported.

Source: [MECA specification resources](https://meca.zip/).
