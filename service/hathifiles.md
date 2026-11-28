# HathiTrust Hathifiles

## Service and data

Hathifiles are HathiTrust's full and incremental bibliographic/volume catalogue
exports. They include catalogue attributes and access/rights information; these
exports are not the full text of the books.

Enable facade feature `service-hathifiles`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## File hierarchy and dataset boundaries

The following paths are concrete examples or explicitly marked templates, not
an exhaustive live inventory. This section describes the selected upstream
representation; it does not add automatic corpus discovery or parsing.

```text
https://www.hathitrust.org/files/hathifiles/
  hathi_file_list.json
  hathi_full_<YYYYMMDD>.txt.gz       # full inventory
  hathi_upd_<YYYYMMDD>.txt.gz        # incremental additions/updates
# Concrete dates must come from the selected listing; this is a naming template.
```

The publisher's listing generator exposes filename, full flag, size, created/modified
timestamps and URL. It uses a rolling retention window (70 days by default in the
inspected code), so this endpoint is not a permanent archive. Save selected files
locally. Hathifiles contain tab-delimited fields; the publisher's current field
definition lists 26 columns, starting with `htid`. Preserve identifiers as strings and
pin the field/header specification instead of inferring types from numeric-looking
values.

## Sharding strategy and current library boundary

`HathifilesService::catalog()` retains the listing and exposes every advertised full/update file without date or rights filtering. `open(name)` returns original sequential gzip bytes. Gzip and TSV decoding, baseline selection, update ordering and reconciliation remain caller responsibilities. Parallel fetches of separate update files do not authorize applying their state changes out of order.

| Boundary | Meaning |
| --- | --- |
| Acquisition | One gzip full or update file from a frozen listing. |
| Logical grouping | Volume identifier htid, retaining each update's source date. |
| Proposed shard | Stable htid buckets after TSV parsing, separately versioned full/update events. |
| Unsafe boundary | Append all listed files as disjoint rows, or split gzip at arbitrary byte offsets. |

Recommended application plan (not a new provider request option):

Choose one full baseline and the subsequent update files through an explicit cutoff.
Save the listing, filenames, dates and digests, and check the required update sequence
before processing. Stream gzip and parse tab-separated records with the pinned column
order and explicit header policy; do not assume CSV quoting rules or that each file
carries a header. Preserve empty fields and multi-valued identifier strings. Partition
parsed records using SHA-256 of UTF-8 htid, first eight bytes unsigned big-endian modulo
fixed N. A single gzip stream remains a sequential acquisition/decompression boundary
even if downstream buckets are processed concurrently.

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

Maintain an immutable event view keyed by source file/date and htid, then derive a
current view by applying updates in source-date order with deterministic within-file
duplicate handling. Multiple full files overlap and must not be concatenated. Absence
from a daily update is not deletion. Reconcile against a later complete baseline to
detect removals; missing update files require recovery from a newer full snapshot.
Rights timestamps describe rights metadata and should not replace the source-file
sequence as a general change cursor.

Reconcile input rows, distinct htids and output events separately. Verify column count
and leading zeros, report duplicate htids, and confirm chronological update application.
Record deletions found during full-baseline reconciliation and do not claim full-text
coverage from metadata counts.

These are design recommendations based on the existing provider source and
publisher references. No live corpus conversion, full inventory acquisition or
sharding performance measurement was performed for this documentation.

- [Publisher field definitions and generation](https://github.com/hathitrust/hathifiles)
- [Publisher listing generator](https://github.com/hathitrust/hathifiles/blob/main/jobs/update_hathifile_listing.rb)
- [HathiTrust data page](https://www.hathitrust.org/hathifiles)

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
