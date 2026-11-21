# CourtListener bulk snapshots

## Service and data

CourtListener provides legal data including courts, opinions and opinion
clusters. Its bulk export stores dated table snapshots as bzip2-compressed CSV,
with companion SQL schema objects. This provider discovers snapshots and decodes
a selected table's bytes.

Enable facade feature `service-courtlistener`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Date shards and input membership

For historical ingestion, **one export date is one application shard**. Each
shard writes a separate Lance dataset for each selected table. Different dates
are independent conversion/retry units even though their records overlap:
these are full historical snapshots, not disjoint portions of one snapshot.

The default text-corpus scope is `opinions` only. A date shard reads exactly:

```text
s3://com-courtlistener-storage/bulk-data/opinions-<date>.csv.bz2
```

The associated SQL schema and a frozen acquisition manifest are provenance
artifacts, not additional opinion rows. Record the actual available SQL key;
`schema_object()` constructs a same-date candidate but does not discover or
fetch it. Some historical CSV/SQL dates differ. If no verified companion exists,
record that explicitly rather than silently borrowing another date's schema.
No PDF, audio, embedding download, HTML asset fetch or metadata join is implied.
The opinion CSV's text/HTML/XML columns are retained as exported.

An optional relational scope selects an explicit table allowlist for the date:

| Input | Role | Independent output |
| --- | --- | --- |
| `opinions-<date>.csv.bz2` | Opinion records and exported content representations | `opinions.lance` |
| `opinion-clusters-<date>.csv.bz2` | Opinion group/case metadata | `opinion-clusters.lance` |
| `dockets-<date>.csv.bz2` | Docket metadata | `dockets.lance` |
| `courts-<date>.csv.bz2` | Court metadata | `courts.lance` |
| Other explicitly selected dated CSVs | Their own table entities | `<table>.lance` |
| Verified companion `schema-*.sql` | Source SQL types and export provenance | Retained artifact, no Lance rows |

Discover actual keys rather than generating an assumed complete date/table grid.
Missing required tables fail that shard's completion check; optional tables are
reported as absent. Different table schemas must not be combined into one
strict-schema scan. Same-date tables were exported while the database was live,
so report missing cross-table references instead of silently dropping records.

### Observed historical inventory

A read-only public S3 listing on 2026-11-21 found 41 distinct dates among direct
`bulk-data/<table>-<date>.csv.bz2` keys, excluding `randoms/`, SQL files and
embeddings. These are not 41 complete table bundles. `opinions` has 37 dated
objects from 2022-08-02 through 2026-09-30, totaling 1,513,770,534,196 compressed
bytes. The 2024-03-01 object is 14 bytes and was directly verified to decode to
zero bytes: report it as an empty upstream export and skip it, yielding **36
nonempty opinions date-shard candidates**. Nonempty object size is not proof of
successful record validation; reconcile every candidate during conversion.

The 2026-09-30 date has 33 CSV objects totaling 65,086,860,152 compressed bytes;
its opinions object alone is 55,252,643,429 bytes. This is CSV export size, not
uncompressed/Lance size or the size of referenced external documents. Freeze a
new inventory before execution; these observations are not permanent counts.

## Lance dataset organization

Proposed application output, not an automatic accessor behavior:

```text
<destination>/courtlistener/<acquisition-id>/
  manifest.json
  2026-06-30/
    opinions.lance/
    provenance/                       # selected SQL, header and validation report
  2026-09-30/
    opinions.lance/
    opinion-clusters.lance/            # optional relational scope
    dockets.lance/                     # optional relational scope
    courts.lance/                      # optional relational scope
    provenance/
```

For opinions-only scope, one date produces one dataset. For relational scope,
a date is a task group containing one independent dataset/job per table; a
directory holding several datasets is not itself a Lance dataset. Publish the
group complete only after every required table passes validation. Do not append
historical dates into the latest dataset or join the tables during raw ingestion.
Use `opinions.cluster_id -> opinion-clusters.id`,
`opinion-clusters.docket_id -> dockets.id` and `dockets.court_id -> courts.id`
for a separate, explicitly requested derived join.

Manifest entries pin date, table, exact source URI, compressed size, ETag/version
when available, downloaded-byte SHA-256, actual SQL companion, header/schema
fingerprint, parser contract version and destination. ETag is an object validator,
not necessarily a content hash. Reuse frozen membership for retries. A changed
source object or schema creates a new acquisition instead of overwriting a
completed dataset. Record final row counts and completion/error status per entry.

## Proposed raw Lance schema

The provider supplies **no fixed Arrow or Lance schema**. The following is the
proposed conversion-layer contract; it is not an implemented typed provider API.

Use one row per complete CSV record, preserve every header column and its order,
and represent source values as nullable `Utf8` initially. Preserve unquoted NULL
versus quoted empty string, original numeric spelling and text/HTML/XML contents;
do not infer SQL types or choose a preferred content representation during raw
ingestion. Correct PostgreSQL COPY escaping and quote-aware NULL handling must
be verified in the parser before production; an all-strings schema alone does
not establish those semantics.

The actual header read from the 2026-09-30 opinions object defines these **22
source columns**, all `Utf8?` in this proposed raw schema:

```text
id, date_created, date_modified, author_str, per_curiam, joined_by_str,
type, sha1, page_count, download_url, local_path,
plain_text, html, html_lawbox, html_columbia, html_anon_2020,
xml_harvard, xml_scan, html_with_citations, extracted_by_ocr,
author_id, cluster_id
```

Append four required provenance columns, for **26 output columns** for this header:

| Column | Arrow/Lance type | Meaning |
| --- | --- | --- |
| `_courtlistener_snapshot_date` | `Date32`, non-null | Export date parsed from the selected filename |
| `_courtlistener_table` | `Utf8`, non-null | `opinions`, or the selected table name |
| `_courtlistener_source_uri` | `Utf8`, non-null | Exact original CSV.bz2 object URI |
| `_courtlistener_record_index` | `UInt64`, non-null | Zero-based logical CSV record ordinal, excluding the header |

Reserve this provenance prefix and reject collisions. Each other table follows
the same rule: its own complete header as nullable strings, plus these four
columns. Do not invent a shared schema for courts, dockets and opinions. Capture
the actual historical header per date: do not assume all 36 opinions exports
have the 2026-09-30 columns. A changed header gets a distinct recorded schema
fingerprint and must be reviewed rather than silently dropping fields. Combining
dates later requires an explicit schema-union/migration policy.

Validate non-null/nonempty and unique `id` where the table's source contract
establishes a primary key. Across dates, identity includes snapshot date and
table; `id` alone is not unique across historical datasets. The record ordinal
tracks source position and must not depend on parallel batch arrival order.
Typed SQL mappings or normalized/joined training schemas are separate derived
outputs with explicit conversion and failure rules.

## Current library support and execution

| Requirement | Current support / remaining application work |
| --- | --- |
| Discover dates for one table | `CourtListenerBulkAccessor::snapshots(table)` returns sorted unique selectors |
| Select one table/date without listing | `CourtListenerSnapshot::new(table, date).object()` |
| Decode one selected object | `open(ByteReadRequest)` streams decoded CSV bytes through Bzip2 |
| Partition selected dates of a compatible table schema | `records(format)` wraps a caller-supplied CSV format in `FileSetAccessor`; an explicit `FileSetRequest` has one file partition per selected object |
| Execute those file partitions concurrently | Shared `ParallelReadLayer`, with independent reader/decoder state per file |
| Date task groups and independent output datasets | Application planner/job orchestration; not supplied by the accessor |
| Frozen inventory, empty-export policy and provenance columns | Application conversion work; not automatic |
| Table/date-specific schema and COPY CSV semantics | Explicit caller/parser contract, not a fixed provider schema |

Thus existing library primitives support date-file partitioning, but there is no
CourtListener-specific `shard=date` request or end-to-end date-to-Lance planner.
For independent destination datasets, submit one conversion per selected
`(table, date)` and use job-level concurrency. A single parallel file-set scan
merges records into one reader; it does **not** route dates to separate datasets.
Only combine files in such a scan when their schemas are compatible and that
combined output is explicitly desired.

A conceptual selection using existing public types is:

```rust,ignore
let selected = snapshots.snapshots("opinions").await?;
// Freeze/filter this inventory, including the explicit empty-export policy.
let files = FileSetRequest::new(selected.into_iter().map(|s| s.object()));
let records = snapshots.records(csv_format); // caller-defined schema and dialect
// Plan file partitions, or apply ParallelReadLayer for a combined compatible scan.
let plan = records.plan_scan(files).await?;
```

This illustrates APIs only; it is not a complete converter and does not attach
provenance or create Lance outputs. See the existing
[record-planning test](tests/courtlistener.rs) and
[parallel-reading guide](../docs/parallel-read.md).

Bound concurrent date jobs by decompression CPU, memory, write bandwidth and
source limits; begin with a small measured concurrency rather than launching
all dates at once. Within one file, the current provider advertises
`read_with_range=false` and `read_parallel=false`; no record-range checkpoint or
internal shard planner is supplied. Bzip2 block-level parallel decompression is
possible with other implementations but is not implemented by this provider.
Arbitrary HTTP ranges, compressed blocks, newlines, Arrow batches and Lance
fragments are not independently framed CSV record shards. No Parquet staging or
primary-key hash repartition is required for the date-shard design.

## Refresh, validation and evidence

A new date adds a separate historical dataset; it does not replace or append to
an older date's output. Reconcile source record count, emitted rows, unique
primary keys where applicable, NULL/empty round-trips and escaped multiline text.
Validate outputs by reopening them and checking schema/provenance and counts.
A date group is complete only when its required outputs are complete; report
upstream empty exports separately from successful conversions.

This design was checked against provider code, the record-planning test,
publisher references, a full public prefix listing and a bounded 2 MiB range
read of the latest opinions file. The range read decoded its 22-column header;
the empty historical opinions export was separately read and decompressed.
No full CSV parse, corpus conversion, parallel benchmark or production deployment
was performed. The proposed provenance columns, schema and orchestration still
require conversion-layer implementation/verification.

- [Bulk data documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
- [Bulk object browser](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/)
- [Publisher export script](https://github.com/freelawproject/courtlistener/blob/main/scripts/make_bulk_data.sh)

## Request and response

Configure byte storage rooted above `bulk-data/`, then use
`CourtListenerBulkAccessor::new(storage)`.

| Request | Response |
| --- | --- |
| `snapshots(table)` | Sorted, unique `Vec<CourtListenerSnapshot>`; requires listing |
| `CourtListenerSnapshot::new(table, "YYYY-MM-DD")` | Validated selector, no I/O |
| `open(ByteReadRequest::new(snapshot.object()))` | Sequential decoded CSV `ByteReader`; explicit reads do not require listing |

`object()` is `bulk-data/{table}-{date}.csv.bz2`; `schema_object()` identifies
`bulk-data/schema-{date}.sql` without fetching it. Input byte options are forwarded.
Decoded content length is unknown and ranges are unsupported. CSV quoting,
backslash escapes, multiline values and NULL/empty distinctions remain intact.
The provider defines no Arrow schema. Same-date tables are not an atomic database
snapshot. For public S3, callers configure the `com-courtlistener-storage` bucket,
region and unsigned access on their OpenDAL operator; local mirrors work too.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ByteReadRequest, Result,
    service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot},
    storage::{OpenDalAccessor, opendal},
};

async fn read_table(operator: opendal::Operator, table: &str, date: &str) -> Result<()> {
    let snapshots = CourtListenerBulkAccessor::new(OpenDalAccessor::new(operator));
    let selected = CourtListenerSnapshot::new(table, date)?;
    println!(
        "CSV: {}, SQL: {}",
        selected.object().locator(),
        selected.schema_object().locator()
    );
    let mut reader = snapshots
        .open(ByteReadRequest::new(selected.object()))
        .await?;
    if let Some(bytes) = reader.next().await? {
        println!("decoded CSV chunk: {} bytes", bytes.len());
    }
    Ok(())
}
```

For a table such as `courts`, the example identifies the CSV/SQL objects and
reads decompressed CSV bytes. A chunk is not a row. Use a consumer matching the
export's SQL/CSV semantics before turning these bytes into Arrow columns.

Source: [CourtListener bulk data](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data).
