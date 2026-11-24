# CourtListener bulk snapshots

## Service and data

CourtListener provides legal data including courts, opinions and opinion
clusters. Its bulk export stores dated table snapshots as bzip2-compressed CSV,
with companion SQL schema objects. This provider discovers snapshots, decodes
selected tables, emits typed Arrow records and joins the four opinion-centric tables by date. Text, HTML and XML
remain inline. An optional document-URL adapter exposes file references for
sink-side PDF downloads without fetching the documents.

Enable facade feature `service-courtlistener`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Date shards and input membership

For historical ingestion, **one export date is one application shard and one
joined Lance dataset**. Different dates are independent conversion/retry units
even though their records overlap: these are full historical snapshots, not
disjoint portions of one snapshot.

Each date shard requires these four same-date CSV inputs:

| Input under `s3://com-courtlistener-storage/bulk-data/` | Role in each output row |
| --- | --- |
| `opinions-<date>.csv.bz2` | Driving table; one opinion becomes one output row |
| `opinion-clusters-<date>.csv.bz2` | Nested `cluster`, matched by `opinions.cluster_id = opinion-clusters.id` |
| `dockets-<date>.csv.bz2` | Nested `docket`, matched by `opinion-clusters.docket_id = dockets.id` |
| `courts-<date>.csv.bz2` | Nested `court`, matched by `dockets.court_id = courts.id` |

The associated SQL schema and frozen acquisition manifest are provenance
artifacts, not output rows. Record the actual available SQL key;
`schema_object()` constructs a same-date candidate but does not discover or
fetch it. Some historical CSV/SQL dates differ. If no verified companion exists,
record that explicitly rather than silently borrowing another date's schema.
Document bytes are downloaded by the sink, not the source. The optional
`with_document_urls(media_root)` adapter resolves PDF/original-file references;
it does not fetch PDF, audio, embeddings or HTML assets. Preserve every
exported source column in its corresponding struct, including the opinion's
text/HTML/XML representations. Other bulk tables are outside this opinion-centric
contract; this output is not a lossless database dump of all 33 tables.

Freeze actual date/table membership rather than assuming every opinions date
has the other three inputs. A missing required file prevents the joined shard
from completing; do not silently substitute another date or downgrade to an
opinions-only output. An empty opinions export is reported and skipped. The 36
nonempty opinions dates below are candidates, not 36 verified complete four-table
bundles. Same-date tables were exported while the database was live, so missing
individual foreign-key matches are allowed and reported under the join rules.

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

## Lance dataset organization and join rules

Proposed application output, not an automatic accessor behavior:

```text
<destination>/courtlistener/<acquisition-id>/
  manifest.json
  2026-06-30.lance/
  2026-09-30.lance/
  provenance/
    2026-06-30/                      # selected SQL, headers and validation report
    2026-09-30/
```

There is exactly **one published Lance dataset per date**, with opinions as the
driving table. Join the other three tables by left joins in this order:

```sql
opinions o
LEFT JOIN opinion_clusters c ON o.cluster_id = c.id
LEFT JOIN dockets d ON c.docket_id = d.id
LEFT JOIN courts k ON d.court_id = k.id
```

These SQL names are logical aliases for the selected CSV objects. Require
nonempty unique `id` values in all four inputs and reject duplicate join keys
before publication, so the joins cannot multiply opinion rows. Compare join keys using the explicit types below: numeric opinion/cluster/
docket IDs use exact checked integer parsing, while court IDs remain exact
strings. Reject lossy conversions and do not trim or case-fold textual keys. A null/empty or unmatched foreign key yields a null related struct and an
orphan count. If `cluster` is missing, `docket` and `court` are null; if `docket`
is missing, `court` is null. A present related row remains a present struct even
if some of its fields are null. Shared clusters/dockets/courts are repeated in
each opinion that references them. Unreferenced related rows are counted in the
validation report but do not create extra opinion rows.

Use bounded-memory joins: stage/index related tables in caller-owned temporary
storage or use a join engine with spilling; do not assume the docket/cluster
maps fit in memory. Stream opinions through those joins into Arrow batches and
one Lance writer. Temporary join artifacts are implementation details, not
additional published Lance datasets. Run different dates concurrently with
separate destinations, and bound aggregate CPU/memory/I/O. Record output row
count equal to the parsed opinions row count before marking a date complete.

Manifest entries pin all four exact source URIs, export date, compressed sizes,
ETag/version when available, downloaded-byte SHA-256, actual SQL companion,
per-table header/schema fingerprints, parser/join contract version and the one
destination. ETag is an object validator, not necessarily a content hash. Reuse
frozen inputs for retries. Changed source bytes/schema create a new acquisition
instead of overwriting a completed dataset. Publish completion only after the
joined output has been reopened and validated.

## Joined Arrow schema

`CourtListenerSchemas::default()` implements the Arrow schema below, pinned
to the 2026-09-30 export. `CourtListenerDatePartitionAccessor` implements the
four-table join. Lance writing and publication remain caller responsibilities.
The example below lists every field observed in bounded reads of the four
2026-09-30 CSV headers. `?` means nullable. Parent struct nullability and child
field nullability are separate: `cluster.id` is required when `cluster` exists,
but `cluster` itself may be null after a missing foreign-key match.

Types and source-field nullability are derived from the actual companion
[`schema-2026-09-30.sql`](https://com-courtlistener-storage.s3.us-west-2.amazonaws.com/bulk-data/schema-2026-09-30.sql),
restricted to the columns present in each CSV header. The SQL schema contains
additional database columns that are not exported; do not manufacture them as
null output fields. These are the explicit conversion rules:

| Source/semantic field | Arrow/Lance output | Conversion policy |
| --- | --- | --- |
| `timestamp with time zone` | `Timestamp(Microsecond, UTC)` | Parse offsets, normalize the instant to UTC and retain microsecond precision; never truncate to a date |
| SQL `date` | `Date32` | Parse a calendar date without assigning a time or timezone |
| SQL `boolean` | `Boolean` | Parse PostgreSQL COPY `t`/`f` exactly; NULL stays NULL only for nullable fields |
| Integer primary/foreign identifiers (`id`, `*_id`) | `Int64` | Widen SQL integer IDs; retain nullable foreign keys and validate referential key types |
| SQL `smallint` | `Int16` | Preserve signed numeric codes/IDs unless listed as a semantic count below |
| Other SQL `integer` | `Int32` | Preserve signed category/code values rather than treating all integers as counts |
| `page_count`, `citation_count`, `scdb_votes_majority`, `scdb_votes_minority`, `view_count`, `ia_upload_failure_count` | `UInt64` | Explicit semantic overrides for nonnegative counts; reject negatives, fractions and overflow |
| SQL `double precision` (`court.position`) | `Float64` | Preserve the numeric ordering value; reject non-finite values under this contract |
| SQL text/varchar | `Utf8` | Preserve text and textual identifiers/codes, including leading zeros; do not guess types from names |

In particular, `court.id`, `docket.court_id`, `docket.appeal_from_id`,
`court.parent_court_id`, `court.fjc_court_id`, `docket.pacer_case_id` and
`docket.federal_dn_office_code` remain strings. Court identifiers must not be
converted to integer IDs. `docket.source` and `docket.docket_number_source`
are numeric codes, while `cluster.source` and `opinion.type` are text codes.
`cluster.other_dates` remains free text despite its name.

Child `?` markers below follow the selected SQL column's NULL constraint, even
inside nullable parent structs. A missing join yields a null parent, not a
fabricated struct with nulls in required children. An exported empty string is
valid for required text; do not conflate it with SQL NULL.

Parse with PostgreSQL COPY CSV semantics: header, quoted fields, backslash
escapes, embedded newlines, and **unquoted NULL versus quoted empty string**.
Do not trim/normalize text or silently cast invalid values to null. Reject NULL
in required fields; empty strings in typed non-text fields are conversion errors.
Reject malformed/out-of-range dates, timezone-free timestamps, fractional or
overflowing integers, invalid booleans and negative counts. Timestamp conversion
must not discard precision beyond microseconds: fail such inputs pending an
explicit precision policy. Preserve the original source object and record
ordinal in error diagnostics. Record the failed date task without publishing a
partial dataset as complete; do not drop bad rows. These parser/type guarantees
are implemented by the typed table reader;
configuring generic all-string CSV inference does not implement this contract.

```text
snapshot_date: Date32                       # required; selected export date
opinion: Struct<
  id: Int64
  date_created: Timestamp(Microsecond, UTC)
  date_modified: Timestamp(Microsecond, UTC)
  author_str: Utf8
  per_curiam: Boolean
  joined_by_str: Utf8
  type: Utf8
  sha1: Utf8
  page_count: UInt64?
  download_url: Utf8?
  local_path: Utf8
  plain_text: Utf8
  html: Utf8
  html_lawbox: Utf8
  html_columbia: Utf8
  html_anon_2020: Utf8
  xml_harvard: Utf8
  xml_scan: Utf8
  html_with_citations: Utf8
  extracted_by_ocr: Boolean
  author_id: Int64?
  cluster_id: Int64
>
cluster: Struct?<
  id: Int64
  date_created: Timestamp(Microsecond, UTC)
  date_modified: Timestamp(Microsecond, UTC)
  judges: Utf8
  date_filed: Date32
  date_filed_is_approximate: Boolean
  slug: Utf8?
  case_name_short: Utf8
  case_name: Utf8
  case_name_full: Utf8
  scdb_id: Utf8
  scdb_decision_direction: Int32?
  scdb_votes_majority: UInt64?
  scdb_votes_minority: UInt64?
  source: Utf8
  procedural_history: Utf8
  attorneys: Utf8
  nature_of_suit: Utf8
  posture: Utf8
  syllabus: Utf8
  headnotes: Utf8
  summary: Utf8
  disposition: Utf8
  history: Utf8
  other_dates: Utf8
  cross_reference: Utf8
  correction: Utf8
  citation_count: UInt64
  precedential_status: Utf8
  date_blocked: Date32?
  blocked: Boolean
  filepath_json_harvard: Utf8
  filepath_pdf_harvard: Utf8
  docket_id: Int64
  arguments: Utf8
  headmatter: Utf8
>
docket: Struct?<
  id: Int64
  date_created: Timestamp(Microsecond, UTC)
  date_modified: Timestamp(Microsecond, UTC)
  source: Int16
  appeal_from_str: Utf8
  assigned_to_str: Utf8
  referred_to_str: Utf8
  panel_str: Utf8
  date_last_index: Timestamp(Microsecond, UTC)?
  date_cert_granted: Date32?
  date_cert_denied: Date32?
  date_argued: Date32?
  date_reargued: Date32?
  date_reargument_denied: Date32?
  date_filed: Date32?
  date_terminated: Date32?
  date_last_filing: Date32?
  case_name_short: Utf8
  case_name: Utf8
  case_name_full: Utf8
  slug: Utf8
  docket_number: Utf8?
  docket_number_core: Utf8
  pacer_case_id: Utf8?
  cause: Utf8
  nature_of_suit: Utf8
  jury_demand: Utf8
  jurisdiction_type: Utf8
  appellate_fee_status: Utf8
  appellate_case_type_information: Utf8
  mdl_status: Utf8
  filepath_local: Utf8
  filepath_ia: Utf8
  filepath_ia_json: Utf8
  ia_upload_failure_count: UInt64?
  ia_needs_upload: Boolean?
  ia_date_first_change: Timestamp(Microsecond, UTC)?
  view_count: UInt64
  date_blocked: Date32?
  blocked: Boolean
  appeal_from_id: Utf8?
  assigned_to_id: Int64?
  court_id: Utf8
  idb_data_id: Int64?
  originating_court_information_id: Int64?
  referred_to_id: Int64?
  federal_dn_case_type: Utf8
  federal_dn_office_code: Utf8
  federal_dn_judge_initials_assigned: Utf8
  federal_dn_judge_initials_referred: Utf8
  federal_defendant_number: Int16?
  parent_docket_id: Int64?
  docket_number_raw: Utf8
  docket_number_source: Int16
>
court: Struct?<
  id: Utf8
  pacer_court_id: Int16?
  pacer_has_rss_feed: Boolean?
  pacer_rss_entry_types: Utf8
  date_last_pacer_contact: Timestamp(Microsecond, UTC)?
  fjc_court_id: Utf8
  date_modified: Timestamp(Microsecond, UTC)
  in_use: Boolean
  has_opinion_scraper: Boolean
  has_oral_argument_scraper: Boolean
  position: Float64
  citation_string: Utf8
  short_name: Utf8
  full_name: Utf8
  url: Utf8
  start_date: Date32?
  end_date: Date32?
  jurisdiction: Utf8
  notes: Utf8
  parent_court_id: Utf8?
>
provenance: Struct<                        # required
  opinions_source_uri: Utf8
  clusters_source_uri: Utf8
  dockets_source_uri: Utf8
  courts_source_uri: Utf8
  opinion_record_index: UInt64
  cluster_record_index: UInt64?
  docket_record_index: UInt64?
  court_record_index: UInt64?
>
```

All provenance URIs identify the four pinned original CSV.bz2 objects, even when
a related row is unmatched. Record indexes are zero-based logical CSV record
ordinals excluding headers, assigned before joining; a nullable index is null
exactly when its related struct is null. They do not depend on parallel batch
arrival order. The manifest records hashes, parser/schema version and SQL
provenance once per acquisition rather than duplicating them in every row.

For this observed header, `opinion` has 22 children, `cluster` 36, `docket` 54
and `court` 20. The top level has six fields: `snapshot_date`, the four entity
structs and `provenance`. Every exported source field remains inside its source
struct;
identically named fields such as `id` and `date_modified` do not collide. No
source columns are silently omitted or replaced with only extracted text.

Historical dates must record and validate their actual headers and source SQL
types/nullability. The mapping above is pinned to the observed 2026-09-30 export,
not a blanket assertion that every historical database schema was identical.
Do not assume all 36 candidate opinions dates match this schema. A changed
header, source type or NULL constraint must be
reviewed and represented in the corresponding struct with a recorded schema
fingerprint; fail on unapproved drift rather than silently dropping fields.
Combining historical datasets later requires an explicit schema-union/migration
policy. Row identity within one date is `opinion.id`; across acquisitions/dates
include acquisition identity and `snapshot_date`.

## Current library support and execution

| Requirement | Current support / remaining application work |
| --- | --- |
| Discover dates for one table | `CourtListenerBulkAccessor::snapshots(table)` returns sorted unique selectors |
| Select one table/date without listing | `CourtListenerSnapshot::new(table, date).object()` |
| Decode one selected object | `open(ByteReadRequest)` streams decoded CSV bytes through Bzip2 |
| Partition selected dates of a compatible table schema | `records(format)` wraps a caller-supplied CSV format in `FileSetAccessor`; an explicit `FileSetRequest` has one file partition per selected object |
| Execute those file partitions concurrently | Shared `ParallelReadLayer`, with independent reader/decoder state per file |
| One joined Arrow stream per date | `CourtListenerDatePartitionAccessor`; caller supplies the Lance writer and destination |
| Provenance columns | Date reader includes original source URIs and per-table record ordinals |
| Frozen inventory and empty-export policy | Caller acquisition/publication work |
| Typed COPY CSV parsing | `CourtListenerTableAccessor`, with defaults pinned to 2026-09-30; historical overrides are explicit |
| XML, text and HTML | Preserved inline in the opinion struct, without choosing one representation or discarding the others |
| PDF/original-file references | `with_document_urls(media_root)` adds document URLs; the sink downloads bytes |

The date reader accepts a `YYYY-MM-DD` locator and exposes one lazy native
partition for that date. Submit one joined conversion task per date and supply
one sink destination. The reader imports all four tables concurrently and
joins them; destination routing and Lance publication remain with the caller.
A parallel file-set scan alone neither joins different tables nor
routes dates to separate outputs; retain one schema per source-table scan.

A conceptual selection using existing public types is:

```rust,ignore
let selected = snapshots.snapshots("opinions").await?;
// Freeze/filter this inventory, including the explicit empty-export policy.
let files = FileSetRequest::new(selected.into_iter().map(|s| s.object()));
let records = snapshots.records(csv_format); // caller-defined schema and dialect
// Plan file partitions, or apply ParallelReadLayer for a combined compatible scan.
let plan = records.plan_scan(files).await?;
```

This illustrates same-table file partitioning only; it does not implement the
four-table join, provenance generation or one-dataset-per-date output. See the existing
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
A date shard is complete only when its one joined output is validated. Check
left-join row-count preservation, unique opinion IDs, every nullable struct and
its provenance index, and orphan/unreferenced-row counts. Report upstream empty
exports separately from successful conversions. Validate each type family,
integer bounds and negative-count rejection, timezone-offset/microsecond
round-trips, required versus nullable fields, booleans and numeric/text join-key
compatibility. Invalid typed values fail the date task instead of becoming null
or disappearing from row counts.

This design was checked against provider code, the record-planning test,
publisher references, a full public prefix listing and a bounded 2 MiB range
read of the latest opinions file plus bounded reads (at most 1 MiB per object)
of all four join inputs. These decoded the 22/36/54/20-column source headers.
The 555,339-byte same-date SQL schema was fetched to verify every documented
source type and NULL constraint;
the empty historical opinions export was separately read and decompressed.
That inventory inspection did not perform a full CSV parse, corpus conversion,
parallel benchmark or production deployment. Typed nested schemas, joins and
row provenance are now implemented and covered by fixture tests. Corpus-scale
validation, durable acquisition manifests and sink orchestration remain caller work.

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
The byte accessor alone defines no Arrow schema; typed table/date accessors
provide the schema above. Same-date tables are not an atomic database snapshot.
For public S3, callers configure the `com-courtlistener-storage` bucket,
region and unsigned access on their OpenDAL operator; local mirrors work too.

## Standalone SQL import

The [standalone SQL importer](../metadata_import/courtlistener/ingest-courtlistener.sql)
creates and populates all four typed tables directly from the HTTPS CSV.bz2 exports.
See its [instructions](../metadata_import/courtlistener/README.md) for extensions,
configuration and differences from the prepared-reader workflow.

## Typed table records

`CourtListenerTableAccessor` provides strict typed records for `opinions`,
`opinion-clusters`, `dockets` and `courts`. `CourtListenerSchemas::default()` pins
the 22/36/54/20 exported fields and SQL nullability of the 2026-09-30 snapshot.
Use `table_schema(table)` to inspect a flat table schema, and `schemas.schema()`
for the nested opinion/cluster/docket/court output contract described in
[the date-shard design](https://github.com/dentiny/open-arrow-access/pull/617).

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, Result};
use open_arrow_access::service::courtlistener::CourtListenerTableAccessor;
use open_arrow_access::storage::OpenDalAccessor;

async fn read_opinions(storage: OpenDalAccessor) -> Result<()> {
    let source = CourtListenerTableAccessor::new(storage);
    let mut reader = source.open(ReadRequest::new(
        "bulk-data/opinions-2026-09-30.csv.bz2",
    )).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} opinions", batch.num_rows());
    }
    Ok(())
}
```

Scalar conversion uses Arrow strict casts (`safe=false`), with COPY-specific
boolean/precision checks; no JSON intermediate rows or custom typed-array
builders are used. Timestamps become UTC microsecond values, dates become `Date32`, booleans and
numeric IDs/codes/counts use explicit numeric types, and text identifiers such
as court IDs retain leading zeros. The parser distinguishes unquoted NULL from
quoted empty strings and supports COPY backslash escaping and multiline fields.
Invalid values (including negative counts), required NULLs and header drift
fail rather than becoming null or losing rows. `with_dialect` accepts an explicit
`CourtListenerCopyDialect` for reviewed historical exports (for example backtick
quotes); quote/escape rules are never guessed from the file. All fields are validated even
when projected away. EOF/errors are terminal; no background producer is spawned.

Historical header/type variants require an explicit reviewed `with_schemas`
contract; this reader does not download or execute source SQL. Input storage
still supplies the compressed bytes. Batches target 8 MiB and at most the requested
row count (capped at 1024); one row may exceed that target. `ScanPlanner` exposes one lazy partition per input object, suitable for
explicit file-set composition. It does not split one bzip2 file into row ranges,
join source tables, discover output destinations or write a Lance dataset.

## One joined stream per date

Enable facade feature `service-courtlistener` (or service-crate feature
`courtlistener`). This includes the single-table and DuckDB joined readers.

`CourtListenerDatePartitionAccessor` reads same-date opinions, opinion-clusters, dockets
and courts. Left joins preserve one row per opinion; missing relationships
produce null structs instead of dropping opinions. The six output fields are
`snapshot_date`, `opinion`, `cluster`, `docket`, `court` and `provenance`. Source
fields and types match `CourtListenerSchemas::schema()` and the design in #617.
Duplicate primary keys fail, preventing accidental multiplication of opinions.
Metadata inspection validates the date, schema and projection without storage I/O
or importing tables. Each complete date is a lazy native scan partition. Caller orchestration runs
dates independently and sends each date stream to one Lance destination.

### Usage and limits

```rust,no_run
use std::num::NonZeroUsize;
use open_arrow_access::{Accessor, ReadRequest, Result};
use open_arrow_access::service::courtlistener::CourtListenerDatePartitionAccessor;
use open_arrow_access::storage::OpenDalAccessor;

async fn read_date(storage: OpenDalAccessor) -> Result<()> {
    let source = CourtListenerDatePartitionAccessor::new(
        storage,
        "s3://com-courtlistener-storage",
    )?;
    let mut rows = source.open(ReadRequest::new("2026-09-30")
        .with_batch_size(NonZeroUsize::new(1024).unwrap())).await?;
    while let Some(batch) = rows.next().await? {
        // Feed this complete nested batch to the one Lance writer for this date.
        println!("{} opinion rows", batch.num_rows());
    }
    Ok(())
}
```

The source root is provenance supplied by the caller and must correspond to the
configured storage root. Unknown/reordered headers fail rather than dropping
fields. Use `CourtListenerSchemas` and `with_schemas` for reviewed historical
header/type variants; the library does not fetch/execute SQL or infer historical
schemas. All four tables are validated even when output projection omits them.
Missing/empty required files, duplicate IDs, malformed COPY records and typed
conversion failures are terminal. All primary keys are checked after import and
before starting the join query; callers must consume through EOF to observe query errors.

The reader follows BHL metadata's two stages. First, it imports the four typed
table streams concurrently into one temporary DuckDB database, with a cloned
connection per table and Arrow appenders. Each table keeps one appender and
flushes once after input ends. A bounded one-batch channel connects asynchronous
reads to the blocking importer. Each table is imported in one transaction,
committed only after reading and appending both succeed. It awaits every importer, including
when one fails. It checks key uniqueness with SQL,
then executes one left-join query and streams Arrow result batches out with
DuckDB's `stream_arrow`/`step` API, following the existing BHL integration.
It does not perform per-opinion key lookups or collect the full query result in
Rust. A temporary directory holds the imported database and join/sort spill
files until the query finishes, then removes them automatically.
DuckDB uses its default memory and thread settings. Scratch usage scales with
the imported date and any join/sort spill. All four inputs are imported before query output starts.

Output uses a bounded one-batch channel from a blocking DuckDB query worker.
Dropping the reader closes that channel. The worker exits when it observes the
closed channel and releases the database and temporary directory; an in-flight
DuckDB operation finishes before cleanup. EOF and query failures are terminal, with
worker failures propagated to the reader. Source-producing tasks are not spawned.

Input batches target 8 MiB; individual CSV records have no byte limit.
DuckDB controls its result chunk size; emitted slices respect the requested row
target without copying arrays. A large record/result chunk may
exceed byte targets. Original per-table record ordinals are imported alongside
the data, and SQL orders the result by opinion ordinal. This global sort may
spill to disk and delays the first output batch; streaming bounds client-side
output buffering, not the work needed to join and sort. This is not a hard
whole-process memory cap or a range-readable source. The reader does not provide
cross-run checkpoints, automatic historical schema migration, a durable manifest,
source hash verification, orphan-count reports or a production Lance commit.
Those acquisition/publication validations remain explicit caller work.

## XML, text and PDF references

Both output schemas are defined in `src/courtlistener/schema.rs`:

| Schema API | Output contract |
| --- | --- |
| `CourtListenerSchemas::schema()` | Six source/provenance fields; already includes text, XML, HTML and original file references |
| `CourtListenerSchemas::schema_with_document_urls()` | The same six fields plus `documents` with resolved file URLs; emitted by `with_document_urls(media_root)` |

Text and XML are not excluded from the default schema. Their original fields
were already exported and remain required `Utf8` children: `opinion.plain_text`,
`opinion.xml_harvard` and `opinion.xml_scan`. The default schema also preserves
`cluster.filepath_pdf_harvard: Utf8`, `opinion.local_path: Utf8` and
`opinion.download_url: Utf8?`. Empty source paths/text remain empty strings;
missing joined clusters remain null structs. A raw PDF path is a reference,
not PDF content. Earlier statements about no PDF downloads do not exclude
these fields or the inline XML/text.

Keep **all** available representations. `opinion.plain_text` is inline extracted
text; `opinion.xml_harvard` and `opinion.xml_scan` are inline XML strings. The
`html*` fields remain unchanged too. Empty source strings remain empty strings.
These fields are representations from different sources/processing steps, not
additional independent opinions or a guarantee of identical content. No
canonical-text selection, XML parsing, OCR or HTML stripping runs in this reader.

PDF bytes are not embedded in the CSV. Preserve the original fields:

| Source field | Meaning / scope |
| --- | --- |
| `opinion.local_path` | Media-relative path to the mirrored original file; it is not guaranteed to be PDF |
| `opinion.download_url` | Original publisher URL, when present; not guaranteed to be PDF or still available |
| `cluster.filepath_pdf_harvard` | Cluster/case-level CAP PDF path; it may cover multiple opinions |

To expose download-ready references, opt into `CourtListenerDocumentAccessor`
using `date_source.with_document_urls(media_root)?`. The configured HTTP(S) root
must correspond to the media storage serving the exported relative paths; it
is independent of the bulk CSV storage/provenance root. The original six fields
and all 132 source children remain intact. A seventh required struct is added:

```text
documents: Struct<
  opinion_file_url: Utf8?
  cluster_pdf_url: Utf8?
>
```

`opinion_file_url` prefers nonempty `opinion.local_path`, falling back to
nonempty `opinion.download_url`. `cluster_pdf_url` uses
`cluster.filepath_pdf_harvard` only. Absolute HTTP(S) references are preserved
verbatim, including signed query strings. Raw relative storage keys are appended
beneath the configured root with URL-encoded path segments; they are not assumed
to be already URL-encoded. Empty references and missing related structs yield
null URLs. No URL is invented for missing content, and there is no cross-scope
PDF fallback. Original CSV paths/URLs are always retained in their source structs.

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, Result};
use open_arrow_access::service::courtlistener::CourtListenerDatePartitionAccessor;
use open_arrow_access::storage::OpenDalAccessor;

async fn read_with_document_urls(
    bulk_storage: OpenDalAccessor,
    media_base_url: &str,
) -> Result<()> {
    let source = CourtListenerDatePartitionAccessor::new(
        bulk_storage, "s3://com-courtlistener-storage",
    )?.with_document_urls(media_base_url)?;
    let mut reader = source.open(ReadRequest::new("2026-09-30")).await?;
    while let Some(batch) = reader.next().await? {
        // Send every original field plus documents URLs to your sink.
        // Sink downloads referenced files and writes PDF/blob content.
        println!("{} opinion rows with document references", batch.num_rows());
    }
    Ok(())
}
```

Metadata and scan planning perform no storage I/O. Reading fetches only the
four bulk tables, never document URLs; it does not check document existence,
MIME type, size, checksum or historical immutability. Batch size, projections,
row order and original provenance retain the date reader's behavior. URL
references are validated even if `documents` is projected away: unsupported
schemes, URL credentials and unsafe relative paths fail the stream rather than
silently losing rows. EOF/errors are terminal. Existing six-field consumers
can continue using the unwrapped date accessor.

The sink owns downloads, credentials, retries, content-type validation, size
limits, hashes and blob writes. Treat `opinion_file_url` as an original-file
reference until the downloaded media type is known. Deduplicate shared cluster
PDFs by source reference/content hash while preserving their association with
each opinion. A missing URL denotes unavailable source content; a failed
download must be reported separately, not converted to a successful empty blob.
The snapshot date dates the CSV, not a guarantee that a live URL still serves
the same historical bytes. Record the actual downloaded object identity/hash
in sink provenance. This adapter does not implement a sink or publication.

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
