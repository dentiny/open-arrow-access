# arXiv bulk PDF and source archives

## Service and data

arXiv distributes research preprints as bulk PDF/source TAR shards, with XML
manifests describing each shard. This provider selects shards and opens archive
entries over caller-configured storage. Bibliographic harvesting is a separate
[OAI-PMH metadata entry point](arxiv-metadata.md).

Enable facade feature `service-arxiv`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Storage is rooted above `pdf/` and `src/`. Configure credentials/requester-pays
on the OpenDAL operator when using upstream S3; the provider does not create a
paid bucket client. It also works with a local mirror of the same keys.

| Library request | Response |
| --- | --- |
| `manifest(ArxivKind::Pdf, Some(yymm), byte_options)` | `Vec<ArxivArchive>` from `pdf/arXiv_pdf_manifest.xml`; `Source` selects `src/arXiv_src_manifest.xml` |
| `stream(&shard, byte_options)` | `TarStream`, sequential headers/bodies, no initial archive-wide scan |
| `archive(&shard, byte_options)` | Indexed `TarArchive`; requires known length and ranges |

`ArxivArchive` fields are `kind`, `filename`, `month: u16` (YYMM),
`sequence: u32`, `size: u64`, `num_items: u64`, `checksum: String`.
`None` selects all months; an absent month yields an empty vector. Manifest
order is retained. MD5 and item counts are metadata, not verified content.
`TarStream::next_entry()` returns `(TarEntry, Box<dyn ByteReader>)`; the body
contains original PDF/source-member bytes, not Arrow rows.

## Usage and results

```rust
use open_arrow_access::{
    ByteReadOptions, Result,
    service::arxiv::{ArxivKind, ArxivService},
    storage::{OpenDalAccessor, opendal},
};

async fn read_bulk(operator: opendal::Operator, month: u16) -> Result<()> {
    let service = ArxivService::new(OpenDalAccessor::new(operator));
    let shards = service
        .manifest(ArxivKind::Pdf, Some(month), ByteReadOptions::default())
        .await?;
    if let Some(shard) = shards.first() {
        println!("shard: {}, {} bytes", shard.filename, shard.size);
        let mut archive = service.stream(shard, ByteReadOptions::default()).await?;
        if let Some((entry, mut body)) = archive.next_entry().await? {
            println!("member: {}", String::from_utf8_lossy(&entry.name));
            if let Some(chunk) = body.next().await? {
                println!("first member chunk: {} bytes", chunk.len());
            }
        }
    }
    Ok(())
}
```

For month `2410` (October 2024), the example selects PDF shards and reads the
first entry's first chunk. To advance a live sequential archive, finish or drop
the current entry reader before requesting another entry.

Source: [arXiv bulk access](https://info.arxiv.org/help/bulk_data.html).
