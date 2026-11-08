# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. Each provider adds its own default-off feature and facade exports.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
Generic storage access and object-store prefix listing belong in `storage`,
not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.

The default-off `huggingface` feature (`service-huggingface` on the facade)
provides `HuggingFaceService::repository` for dataset repository files.
The service owns repository ID, revision, root, endpoint and authentication.
Its byte accessor reuses `storage::OpenDalAccessor`; format accessors decode records.
Floating revisions are not snapshots.
The same service exposes `dataset(HuggingFaceDatasetRequest)` and implements
`Accessor` for that request. This opens one config/split as ordered Arrow batches
from the HF Parquet export. A floating export revision is first pinned with a
small `expand=sha` metadata request (64 KiB response limit); membership and file
reads then use that same commit. Explicit commit requests skip the metadata call.
The selected directory is listed nonrecursively, Parquet paths are sorted, and
existing `ParquetAccessor`/`FileSetAccessor` perform lazy sequential reading,
projection, batching and schema validation. Only the shard list is collected.

A missing complete split fails. `with_partial_exports()` explicitly permits
fallback to `partial-<split>`; full exports take precedence, and permission or
transport errors never trigger fallback. `with_revision` refers to the Parquet
representation, not the original source branch. Native dataset-card parsing and
Viewer row APIs are outside this entry point.

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
Inventory-specific validation lives in `pmc_inventory/validation.rs`.

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

Manifest byte collection uses core `read_limited`. Shard iteration retains its
provider state and uses `StreamReader` for metadata and terminal handling.
## Common Crawl configuration

Enable `service-warc` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Enable `service-warc` for `CommonCrawlWarcRequest` and
`CommonCrawlWarcAccessor`: resolve all WARC paths from a crawl's decoded
`warc.paths.gz` inventory, preserving file order, then read through a supplied
single-file format accessor and the existing sequential FileSet. `resolve`
returns membership without opening record files. The caller supplies gzip for
compressed inventories and file decompression/format parsing separately.
Selected locators are held in memory; inventory lines have a 16 KiB limit.
There is no index, batch planner or parallel execution.

Segment selection is a separate addition.

Manifest byte framing and the 16 KiB per-line bound use core `read_lines`;
path validation and selected-file duplicate checks remain in the provider.

`format-warc` exposes native parser records through `WarcAccessor`. This service
composes Arrow readers through FileSet, so its `format` argument requires a
caller-provided WARC-to-Arrow accessor. Manifest-only `resolve` needs no conversion.
