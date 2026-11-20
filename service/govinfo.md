# GovInfo

## Service and data

GovInfo distributes U.S. Government publications and their metadata. This
provider reads public bulk directories and API collections/packages, with XML,
PDF, text, MODS and archive representations when the selected package offers them.

Enable facade feature `service-govinfo`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## File hierarchy and dataset boundaries

The following paths are concrete examples or explicitly marked templates, not
an exhaustive live inventory. This section describes the selected upstream
representation; it does not add automatic corpus discovery or parsing.

```text
https://www.govinfo.gov/bulkdata/
  FR/2026/
    FR-2026.zip
    01/
      FR-2026-01-02.xml
      FR-2026-01.zip
  CFR/2025/title-1/
    CFR-2025-title1-vol1.xml
    CFR-2025-title-1.zip
  BILLS/119/1/hr/
    BILLS-119hr1001ih.xml
    BILLS-119hr1001eh.xml
  BILLSTATUS/119/hr/...
  <collection>/resources-or-other-subdirectories/...  # discover actual names
```

FR uses year/month issue files, CFR uses edition year/title/volume, and BILLS uses
Congress/session/bill type with versioned bill XML. BILLSTATUS is a distinct collection
and its Congress/type layout differs from BILLS. The paths above are concrete directory
examples plus marked placeholders, not a complete inventory. Year/month/title ZIPs may
cover the same XML published individually. Collection resources provide relevant schemas
and guides.

## Sharding strategy and current library boundary

`GovInfoService::bulk_directory(prefix)` returns direct files in `RemoteFiles<ApiDocument>`; child folders remain in the retained directory metadata and need separate requests. It does not recursively discover the corpus or parse publication XML. `package(id)` and collection paging use the separate GovInfo API. Downloads are sequential bytes. File-level planning below requires an explicit decoder/schema after discovery.

| Boundary | Meaning |
| --- | --- |
| Acquisition | One listed XML or one selected archive representation. |
| Logical grouping | Collection-specific document/package identity, version and source revision. |
| Proposed shard | Disjoint groups of document keys within one collection/schema family. |
| Unsafe boundary | A universal date hierarchy, overlapping ZIP + loose XML ingestion or arbitrary XML offsets. |

Recommended application plan (not a new provider request option):

Recursively enumerate the selected collection using the published JSON or XML directory
endpoints, recording child folders and direct files. Choose either loose XML or archive
members for a scope, then deduplicate by logical document/version and source digest.
Group whole XML documents by collection and measured bytes; retain Congress/session/type
or edition/year/title as useful partition metadata without assuming equal sizes. A large
issue or volume may need a collection-aware XML record parser before finer downstream
partitioning. Record the parent document and element position so derived article/section
rows can be traced and reconstructed.

Pin a manifest containing exact source keys/URLs, logical identities, source
version/date, representation, byte sizes and downloaded-byte digests, parser/schema
version and exact shard membership. Keep unknown metadata and original bytes
where a typed projection cannot preserve them. Archive transport chunks, Arrow
batches and output fragments do not define upstream dataset shards.

For independent record files, compose an explicit record decoder/schema with
file-set planning and the [parallel execution layer](../docs/parallel-read.md).
A file set defaults to one partition per file; native file planning is an
additional opt-in when the inner format supports it. Independent compressed files
can use separate decoder state. This does not imply parallel decompression or
range-readable records within a single compressed stream. Bound active readers,
parser buffers and queued output; parallelism is not a process memory cap.
Apply one compatible schema per scan and preserve source identities independently
of batch arrival order.

## Snapshot refresh and validation

Freeze listing responses and downloaded bytes with acquisition time and content digests.
An old publication path can be corrected, so scanning only the newest date misses
changes. Compare selected-object digests on refresh. Keep BILLS text versions distinct;
do not collapse introduced and engrossed versions merely because the bill number
matches. Bulk access and authenticated package API discovery are separate routes.

Check recursive traversal completeness and count unique selected document/version
identities. Reconcile extracted ZIP membership against loose-file scope to prevent
duplicates. Validate XML with the relevant collection schema where available, and report
corrections, missing documents and unsupported collection variants.

These are design recommendations based on the existing provider source and
publisher references. No live corpus conversion, full inventory acquisition or
sharding performance measurement was performed for this documentation.

- [Bulk root](https://www.govinfo.gov/bulkdata/)
- [Directory endpoint documentation](https://github.com/usgpo/bulk-data)
- [FR example](https://www.govinfo.gov/bulkdata/FR/2026/01)
- [CFR example](https://www.govinfo.gov/bulkdata/CFR/2025/title-1)
- [BILLS example](https://www.govinfo.gov/bulkdata/BILLS/119/1/hr)
- [BILLSTATUS example](https://www.govinfo.gov/bulkdata/BILLSTATUS/119)

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `bulk_directory(prefix)` | `www.govinfo.gov/bulkdata/json/{prefix}` | `RemoteFiles<ApiDocument>` for direct files; child folders remain in metadata |
| `collections()` | `api.govinfo.gov/collections` | `ApiDocument` |
| `packages(collection, start, end, offset_mark, page_size)` | `/collections/{collection}/{start}[/{end}]` plus `offsetMark` and `pageSize` | One `ApiDocument` page |
| `next_page(url)` | API `nextPage`, same origin | Next page |
| `package(id)` | `/packages/{id}/summary` | `RemoteFiles<ApiDocument>` |

Use `new(None)` for public bulk access or `new(Some(key))` for API query-key
configuration; an empty configured key is rejected before I/O. Start package paging with `"*"`; page size is 1–1000. Package file
names are keys in the response's `download` map. Downloads are sequential bytes.
The service does not recursively walk bulk folders. A ZIP and its separately
published XML members are distinct representations of potentially overlapping data.

Illustrative upstream package summary shape (values are examples;
additional fields may be present):

```json
{
  "packageId": "EXAMPLE-1",
  "download": {
    "pdfLink": "https://files.example.org/package.pdf",
    "xmlLink": "https://files.example.org/package.xml"
  }
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::govinfo::GovInfoService};

async fn inspect_bulk_directory(prefix: &str) -> Result<()> {
    let files = GovInfoService::new(None)?.bulk_directory(prefix).await?;
    println!("directory metadata: {}", files.metadata().value());
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

For a prefix such as `FR/2026/01`, the example prints the directory JSON and
reads one direct file chunk. Child directories must be requested separately.

Sources: [GovInfo bulk data](https://github.com/usgpo/bulk-data), [GovInfo API](https://github.com/usgpo/api).
