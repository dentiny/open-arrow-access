# BHL AWS Open Data partitions

Enable facade feature `service-bhl` (or service-crate feature `bhl`).
`BhlOpenData` supplies explicit AWS object membership over caller-bound storage.
It reuses existing native file-set scan planning and bounded parallel execution;
it does not introduce another executor or split one file into arbitrary ranges.

## Discover once, distribute explicit keys

```text
s3://bhl-open-data/                         # us-east-2; anonymous
├── ocr/item-<ItemID>/<page>.txt            # Independent page text objects
├── ocr/part-<PartID>/<page>.txt
├── images/<BarCode>/<page>.jp2             # Independent images
├── scandata/<BarCode>_scandata.xml
└── data/<table>.txt.gz                    # Independent compressed TSV tables
```

The complete source layout and item/part exceptions are documented in the
[official README](https://bhl-open-data.s3.amazonaws.com/README.html) and
[special cases](https://github.com/gbhl/bhl-open-data/blob/main/NOTES.md).
The AWS data set has independent objects, not an official balanced shard list.

`open_data_bucket_operator()` constructs an anonymous bucket-root S3 operator in
`us-east-2` with ambient credential loading disabled and signing skipped.
It performs no I/O. `BhlOpenData::new(storage)` also accepts caller-configured
storage, including an Fs/S3 mirror retaining the bucket-relative keys. Apply
retry/timeout policy to that storage using the normal OpenDAL/accessor layers.

`discover(ListRequest)` lists only the requested directory prefix and forwards
the recursion flag. It returns sorted, unique file keys without opening bodies.
Prefixes must start with `ocr`, `images`, `scandata` or `data`; use complete
directory prefixes such as `ocr/item-346951/`, not a partial ID string.
The returned vector uses memory proportional to selected object count. Prefer
item/part-scoped discovery and persist the resolved keys before distributing jobs.

`partition(keys)` fixes membership to the explicit, nonempty, unique key list,
retaining caller order. It performs **no listing, stat or body I/O**. The returned
`ResolvedFileSet` has those same keys as logical names and a copy of the key
vector as metadata. Listing its empty root is cached; opening an unselected key
fails before touching storage. Paths must remain bucket-relative and cannot
contain traversal, URL query/fragment components or directory entries.

Neither listing nor selection freezes object contents. Save source ETags,
checksums/versions where available and acquisition timestamps with the job's
inventory; keys alone pin membership, not bytes. Assign each key to one job
manifest and keep each job's output destination distinct. Item/part views can
overlap; establish ownership from BHL metadata before assigning keys.

## One selected object becomes one native scan partition

This example accepts a previously saved page-key selection. Import paths below
use the individual crates; through the facade the dataset file-set types are
also available as `open_arrow_access::service::{FileSetAccessor, FileSetRequest}`.

```rust,no_run
use std::num::NonZeroUsize;
use open_arrow_access_core::{
    Accessor, AccessorExt, BinaryDecoder, DocumentAccessor, Result,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::{
    FileSetAccessor, FileSetRequest,
    bhl::{BhlOpenData, open_data_bucket_operator},
};
use open_arrow_access_storage::OpenDalAccessor;

async fn read_partition(saved_keys: Vec<String>) -> Result<()> {
    let source = BhlOpenData::new(OpenDalAccessor::new(open_data_bucket_operator()?));
    let files = source.partition(saved_keys.clone())?; // No discovery on workers
    let records = FileSetAccessor::new(DocumentAccessor::new(files, BinaryDecoder))
        .with_file_planning()
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::MIN,
        ));
    let mut reader = records.open(FileSetRequest::new(saved_keys)).await?;
    while let Some(batch) = reader.next().await? {
        // Each row is one selected object's original data: LargeBinary.
        // Send batches to the application's next processing stage.
        println!("{} object rows", batch.num_rows());
    }
    Ok(())
}
```

The native plan contains one independent partition per selected object. Every
reader has its own cursor; source reads start when that partition is consumed.
`ParallelReadLayer` bounds active partitions and preserves selection order by
default. Plain `plan_scan(...).into_reader()` remains sequential; planning alone
does not start concurrent reads. Byte options, projections and source errors use
the existing file-set/document contracts. A missing object fails the read; it is
not silently skipped or replaced by empty text.

For structured metadata scans, bind a suitable existing format through
`files.records(format)`; explicitly compose gzip decompression and TSV decoding
for `.txt.gz` tables. The object-selection API does not auto-detect or decode
formats, unpack archives, fetch API metadata or infer item/part relationships.

The binary example buffers one **object** per active reader, not the corpus.
This suits small page objects; a large image/archive/table can still consume
large memory. Use byte streaming or an appropriate format reader for those
objects. Raw bytes remain unchanged, including blank/empty pages and invalid
UTF-8. The merged binary output has only `data`; keys remain in the bound
inventory/per-partition reader metadata, not an invented provenance column.

This is the AWS partition/byte-access layer, not the proposed fully joined BHL
page schema. Parsed page IDs, source/snapshot columns and the complete metadata
struct require a separate record mapping/enrichment step before Lance writes.
Generic object reads do not claim that mapping is already implemented.

## Validation

`service/tests/bhl_open_data.rs` verifies scoped discovery, explicit cached
membership without storage I/O, accepted key namespaces and selected-object
reads using the shared byte-accessor contract helper. Generic file-set planning, projection
and parallel execution remain covered by their own tests. Fixtures use
caller-bound filesystem storage; they do not test live AWS coverage or throughput.

A bounded live smoke check also read exactly two public AWS OCR objects through
the anonymous operator and parallel file-set path: item 346951 / page 65077706 /
sequence 14 and item 21356 / page 2839616 / sequence 127. It returned two rows
and 5,659 original bytes without listing the corpus. This verifies those reads,
not complete AWS coverage or sustained throughput. The standalone
smoke-check example used for that validation has since been removed.

See [BHL service](bhl.md), [resolved file sets](remote-files.md) and
[parallel reading](../docs/parallel-read.md) for the shared contracts.

## Page records with supplied metadata

With `service-bhl`, `BhlPageAccessor::new(storage, snapshot_id, provider)` maps
selected item/part OCR filenames to the fixed `page_schema()` and assigns the
provider's one-row metadata StructArray. This constructor accepts supplied metadata. Use `BhlPageAccessor::from_metadata`
to read OCR pages with DuckDB-joined metadata. Invalid keys,
provider errors and schema mismatches fail explicitly. Native partitions use the
shared scan executor. `with_schema_metadata` retains caller provenance in the
plan and emitted batch schemas. The joined constructor attaches metadata provenance automatically and calls the
caller's report hook before each OCR read.

## DuckDB metadata joins

Enable `service-bhl` and load `BhlMetadata::load(storage).await` once.
The 13 AWS gzip TSV tables are imported concurrently into in-memory DuckDB
tables. `metadata.for_each(&page_keys, emit)` runs one join SQL and emits each
page's metadata immediately, matching the fixed Arrow/Lance schema. The first
version assumes sufficient memory, configures no temporary directory, and
explicitly disables DuckDB's external file cache. See the
[metadata guide](bhl-metadata.md) for usage and the integrated OCR reader.

## Persist a corpus inventory and distribute jobs

`BhlOpenData::inventory(snapshot_id, scope)` recursively lists `ocr/` once and
freezes valid OCR keys into canonical lexical order. Choose `BhlOcrScope::Items`,
`Parts` or explicitly `All`. Item and part OCR may represent the same PageID;
`All` keeps both namespace identities and makes no claim of page deduplication.
The inventory rejects duplicate keys and duplicate normalized identities caused
by different zero padding. Images, scandata and TSV objects are not OCR jobs.

```rust,no_run
use std::{fs::File, num::NonZeroUsize};
use open_arrow_access_service::bhl::{
    BhlInventory, BhlOcrScope, BhlOpenData, BhlPageJob, open_data_bucket_operator,
};
use open_arrow_access_storage::OpenDalAccessor;

async fn coordinator() -> Result<(), Box<dyn std::error::Error>> {
    let source = BhlOpenData::new(OpenDalAccessor::new(open_data_bucket_operator()?));
    let inventory = source.inventory("bhl-run-2026-11-20", BhlOcrScope::Items).await?;
    inventory.write_json(File::create("inventory.json")?)?;

    // Retries can load this saved inventory instead of listing AWS again.
    let saved = BhlInventory::read_json(File::open("inventory.json")?)?;
    for job in saved.jobs(NonZeroUsize::new(1_000).unwrap()) {
        job.write_json(File::create(format!("job-{:06}.json", job.index()))?)?;
    }
    Ok(())
}

fn worker_selection() -> Result<(), Box<dyn std::error::Error>> {
    let job = BhlPageJob::read_json(File::open("job-000000.json")?)?;
    let source = BhlOpenData::new(OpenDalAccessor::new(open_data_bucket_operator()?));
    let files = source.partition(job.objects().iter().cloned())?;
    // Use `files` with a byte/format reader, or pass `job.request()` to a
    // BhlPageAccessor::from_metadata configured with job.snapshot_id() and
    // a shared, loaded DuckDB metadata value.
    Ok(())
}
```

Both saved formats are strict version-1 JSON. Inventory stores snapshot ID,
namespace scope and sorted keys. Each job additionally stores the inventory page
count, page limit and zero-based chunk index, plus only that job's keys. Jobs are
generated lazily as disjoint consecutive chunks; the last can be shorter, and an
empty inventory produces none. Loading a job validates its key identities and
scope, index arithmetic and expected chunk length without reading the inventory.

A job is identified by `(snapshot_id, pages_per_job, index)`. Keep the inventory
and sizing unchanged while recording completed jobs and retrying failed jobs.
A standalone job validates its structure but cannot independently prove its keys
belong to the saved inventory, or prove completeness/disjointness of files mixed
from other runs. Jobs generated from one inventory have those membership guarantees. Use one trusted coordinator's job set. Caller-assigned snapshot IDs
freeze membership, not S3 bytes: listing is live and no object-version/checksum
snapshot is created. Publish inventory/job files atomically using the caller's
storage policy and avoid overwriting a published run with different membership.

Coordinator memory is proportional to key count; individual job generation and
worker selection are count-bounded. There are no body downloads or per-page
HEAD/length probes during inventory planning, and no corpus listing on workers.
Load and share `Arc<BhlMetadata>` once per worker process. Create a
`BhlPageAccessor::from_metadata` with `job.snapshot_id()` and that shared value,
then read `job.request()` to join the selected pages and acquire their OCR.
Inventory discovery and job generation do not acquire metadata tables.
