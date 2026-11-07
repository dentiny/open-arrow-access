# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. The
workspace scaffold has no enabled providers or transport dependencies. Each
provider adds its own default-off feature and facade exports.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
Generic storage access and object-store prefix listing belong in `storage`,
not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.

## PMC inventory snapshots (`pmc_inventory`)

Enable `service-pmc-inventory` for `service::pmc_inventory::PmcInventoryAccessor`.
Supply a standard `ReadRequest` naming an explicit inventory `manifest.json`,
for example `inventory-reports/pmc-oa-opendata/metadata/2026-11-02T01-00Z/manifest.json`.
The caller selects the snapshot. The service does not list the bucket, discover
"latest", or fetch any article metadata or content objects. It is independent of
the single-article metadata service and can be enabled and merged separately.

The manifest is read during `open`, bounded to 4 MiB by default (configurable
with `with_manifest_limit`). Its `fileSchema` defines the output CSV columns;
all upstream names, values, and order are retained as nullable Utf8, including
future additional fields. The standard PMC fields `Bucket`, `Key`,
`LastModifiedDate`, and `ETag` are required. A final non-null `metadata_key`
column contains the percent-decoded, validated bucket-relative metadata JSON
key. The original encoded `Key` remains unchanged.

Schema metadata records the manifest object URI, creation timestamp, manifest
version, and complete verbatim manifest JSON. That preserves shard keys, declared
sizes/checksums, the original file schema, and unknown manifest fields. Reader
metadata identifies the manifest as its source. The caller can persist the
manifest and inventory records beside its converted dataset.

Only CSV inventories with gzip-compressed shards are supported. The service
uses the existing `DecompressionLayer<Gzip>` and `CsvAccessor`; it does not
implement another gzip or CSV parser. `next` opens shards lazily and sequentially
in manifest order and retains row order and duplicates. Common batch sizing,
projection, zero-column row counts, and per-open byte I/O options are supported.
Projection is resolved after the manifest supplies the schema, before any CSV
shard opens. Validation still examines Bucket and Key when projection omits them.
Each batch shares the cached output schema, so projection does not copy the raw
manifest per batch. Selected CSV arrays are reused; `metadata_key` is built
directly into an Arrow string builder only when selected.
All validation functions live in `pmc_inventory/validation.rs`.

Missing shards, invalid rows, malformed/truncated CSV/gzip, invalid manifests,
and source errors are terminal. Previously delivered batches stay delivered;
a failed current batch is not returned. Dropping the reader stops future shard
opens. A CSV shard is streamed rather than buffered as one object; batch size is
not an RSS or maximum-field-size bound. No snapshot-wide sorting, deduplication,
licence filtering, or background producer is performed.

The manifest and shard keys pin the requested inventory report, not live article
objects. Declared checksums/sizes are preserved, but are not verified against
content. The service does not freeze backend objects or guarantee they cannot
change. Caller-configured byte access, mirror roots, credentials, and backend
versioning stay outside this provider.

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, service::pmc_inventory::PmcInventoryAccessor};
use open_arrow_access_storage::{OpenDalAccessor, opendal};

async fn inventory(operator: opendal::Operator) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = PmcInventoryAccessor::new(OpenDalAccessor::new(operator))
        .open(ReadRequest::new("inventory-reports/pmc-oa-opendata/metadata/2026-11-02T01-00Z/manifest.json"))
        .await?;
    while let Some(batch) = reader.next().await? {
        // Persist inventory rows or feed metadata_key to a separate article reader.
        println!("{} inventory objects", batch.num_rows());
    }
    Ok(())
}
```

Run `cargo run --example read_pmc_inventory --features service-pmc-inventory --
/mirror/root <dated-manifest-key>` against a mirror preserving manifest/shard keys.
See the [PMC dataset README](https://pmc-oa-opendata.s3.amazonaws.com/README.txt)
and [S3 inventory manifest format](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html).
