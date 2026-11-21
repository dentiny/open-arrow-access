# CourtListener bulk snapshots

## Service and data

CourtListener provides legal data including courts, opinions and opinion
clusters. Its bulk export stores dated table snapshots as bzip2-compressed CSV,
with companion SQL schema objects. This provider discovers snapshots and decodes
a selected table's bytes.

Enable facade feature `service-courtlistener`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

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

Enable facade feature `service-courtlistener-joined` (or service-crate feature
`courtlistener-joined`). The existing `service-courtlistener` single-table API
does not pull in DuckDB.

`CourtListenerDateAccessor` reads same-date opinions, opinion-clusters, dockets
and courts. Left joins preserve one row per opinion; missing relationships
produce null structs instead of dropping opinions. The six output fields are
`snapshot_date`, `opinion`, `cluster`, `docket`, `court` and `provenance`. Source
fields and types match `CourtListenerSchemas::schema()` and the design in #617.
Duplicate primary keys fail, preventing accidental multiplication of opinions.
Each complete date is a lazy native scan partition. Caller orchestration runs
dates independently and sends each date stream to one Lance destination.

### Usage and limits

```rust,no_run
use std::{num::NonZeroUsize, path::PathBuf};
use open_arrow_access::{Accessor, ReadRequest, Result};
use open_arrow_access::service::courtlistener::{
    CourtListenerDateAccessor, CourtListenerJoinOptions,
};
use open_arrow_access::storage::OpenDalAccessor;

async fn read_date(storage: OpenDalAccessor, scratch: PathBuf) -> Result<()> {
    let source = CourtListenerDateAccessor::new(storage, CourtListenerJoinOptions {
        source_root: "s3://com-courtlistener-storage".into(),
        scratch_directory: scratch, // existing directory on caller-owned disk
        dialect: Default::default(), // current COPY quote/escape rules
        memory_limit: NonZeroUsize::new(1024 * 1024 * 1024).unwrap(),
        threads: NonZeroUsize::new(4).unwrap(),
    })?;
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
conversion failures are terminal. Primary-key validation for opinions continues
while streaming; callers must consume through EOF before accepting a dataset.

The reader imports the four validated typed table streams into a temporary
DuckDB database, using DuckDB's Arrow appender. It checks key uniqueness with SQL,
then executes one left-join query and streams Arrow result batches out with
DuckDB's `stream_arrow`/`step` API, following the existing BHL integration.
It does not perform per-opinion key lookups or collect the full query result in
Rust. The database and spill files live under the caller's scratch directory;
`memory_limit` and `threads` configure DuckDB. The memory limit controls engine
operators, not whole-process RSS. Scratch usage scales with the imported date
and any join/sort spill. All four inputs are imported before query output starts.

Output uses a bounded one-batch channel from a blocking DuckDB query worker.
Dropping the reader closes that channel and interrupts the query. The worker
then releases the database and temporary directory; cleanup may finish after
an in-flight native operation exits. EOF and query failures are terminal, with
worker failures propagated to the reader. Source-producing tasks are not spawned.

Input batches target 8 MiB; individual CSV records have no byte limit.
DuckDB controls its result chunk size; emitted slices respect the requested row
target without copying arrays. A large record/result chunk may
exceed byte targets. Original per-table record ordinals are imported alongside
the data, and SQL orders the result by opinion ordinal. This is not a hard
whole-process memory cap or a range-readable source. The reader does not provide
cross-run checkpoints, automatic historical schema migration, a durable manifest,
source hash verification, orphan-count reports or a production Lance commit.
Those acquisition/publication validations remain explicit caller work.

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
