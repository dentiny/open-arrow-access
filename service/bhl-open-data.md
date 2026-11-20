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
Do not conflate this layout with the single Figshare TAR.bz2 export.

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

Generic object reads remain a byte-access layer. For structured page conversion,
enable `service-bhl-metadata`: `bhl::metadata::prepare(storage, source_root, path)`
streams all 13 `data/*.txt.gz` TSV tables into one SQLite snapshot on local/shared
storage. Headers must match the fixed 104-field schema, cells remain lexical
strings, and duplicate numeric singleton IDs fail preparation. Completed tables
can resume after interruption; use one preparer per output path. Publication is
exclusive and an existing completed snapshot is never overwritten.

Prepare once, then give each job the same absolute worker-visible path and
`BhlMetadataInfo.fingerprint`. `BhlPageMetadata::load(path, fingerprint, objects)`
does indexed lookups only for the selected OCR pages. Run this synchronous local
I/O on a blocking worker. Pass `move |identity| metadata.get(identity)` to
`BhlPageAccessor::new` and attach `metadata.schema_metadata()` using
`with_schema_metadata` before moving the provider into the closure.

Item pages join page/item/title; part pages additionally join part and matching
partpage, resolving the parent item from part, partpage, then page. Related
authors, identifiers, subjects, names and DOI rows retain source order and
duplicates. DOI joins use both entity type and numeric ID. Original rights are
preserved independently on item and part. Missing singleton rows become null
children and missing relation rows become empty lists, while the outer metadata
struct is always assigned. Missing joins and parent/sequence conflicts appear in
`bhl.metadata.reconciliation`; rows and OCR bytes are retained.

The snapshot records all source paths, counts and SHA-256 hashes of canonical
JSON cell rows, plus a combined fingerprint. These are decoded-content hashes,
not compressed-object hashes or AWS version IDs. Acquisition spans a time
window, including resumed tables; it does not promise an atomic AWS export.
Keep the published SQLite file immutable and accessible at the identical path on
every worker. Jobs fail on missing/incomplete snapshots or fingerprint mismatch.

Page projections resolve before OCR reads. Selecting `page_id`, metadata or other
columns without `data` performs no OCR object opens or body downloads; only the
selected Arrow columns are built. An empty projection preserves one row per page.
Selecting `data` retains original bytes, storage I/O options and read failures.

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
not complete AWS/Figshare equivalence or sustained throughput. The standalone
smoke-check example used for that validation has since been removed.

See [BHL service](bhl.md), [resolved file sets](remote-files.md) and
[parallel reading](../docs/parallel-read.md) for the shared contracts.
