# Parallel reading and emission policies

`ParallelReadLayer` runs independent partitions on Tokio tasks and defaults to
emitting every partition in declared order. The executor also supports an
explicit unordered emission policy when the entire source is reorderable. FileSet preserves explicit duplicates and selection order; Directory
uses the same validated, sorted discovery as sequential reads.

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_access::{AccessorExt, layers::ParallelReadLayer};

let parallel = files.layer(ParallelReadLayer::new(
    NonZeroUsize::new(4).unwrap(), // resident partitions
    NonZeroUsize::new(2).unwrap(), // queued batches per partition
));
let mut reader = parallel.open(request).await?;
while let Some(batch) = reader.next().await? {
    // Process in the original file and record order.
}
```

The accessor must implement the optional `ScanPlanner` extension. Unsupported
sources fail at compile time instead of silently ignoring parallel settings.
Ordinary `open` on the unwrapped accessor remains sequential. By default, file-set
planning opens the first selected file to fix the schema and creates one partition
per file. Empty file sets remain errors.

`Capability::read_parallel` reports concurrent execution in the current interface.
`ParallelReadLayer` advertises it on the wrapped accessor when `read` is supported,
and on the opened reader. Implementing `ScanPlanner` alone does not advertise it:
planning exposes independent work, while the layer executes that work concurrently.
The flag reports capability even when a request has zero or one partition or the
configured partition limit is one. Inspecting it enables no work and does not
promise a speedup, unordered output, or parallel byte I/O within each reader.

Call `with_file_planning()` on `FileSetAccessor` or `DirectoryAccessor` when the
inner format implements `ScanPlanner`. Native partitions (for example, Parquet
row groups) are flattened in file and record order under the same executor
budget. Only the first file is planned initially; later files are planned when
partition discovery reaches them. Later plan schemas, including empty plans,
must match the first schema. No nested executor or second worker budget is used.
Hugging Face dataset scans enable this mode automatically. Resolved provider
files can opt in with `files.records(format).with_file_planning()`. Common Crawl
WARC and segment accessors expose the same builder when their supplied format
supports native planning.

All partition metadata and batches must match the fixed schema, including
schema metadata. Both `ParallelReadLayer` and sequential `ScanPlan::into_reader()`
reject mismatches, including empty partitions with different schemas. Partition
source identifiers may differ. Format-specific discovery may retain an input
reader, framing state and buffers separately from partition workers.

With P resident partitions and Q queue slots per partition, at most P partition
readers are active and P * Q batches are queued. A worker reserves queue capacity before
calling `next`. A finished worker retains its partition slot until its queue has
drained; a slow first file cannot cause unlimited later files to accumulate.
The consumer can hold additional batches. Decoder allocations, individual batch
sizes and compressed input buffers are not bounded by these queue limits.
Storage's per-reader I/O limit remains separate: P readers may each use their
configured I/O concurrency. This version does not impose a backend-global cap.

Each worker owns its reader and codec state. File-level plans can therefore run
independent compressed files concurrently. Native file planning may frame decoded
bytes sequentially before dispatching independent decode partitions; decompression
is not made parallel by this composition. Decoded bytes retain their order.

Workers begin on first consumption and require an active Tokio runtime. On a
multithreaded runtime decoder work can run on separate runtime threads; a single
thread runtime overlaps asynchronous waits but does not provide CPU parallelism.
Errors and panics terminate the scan, including failures behind a blocked first
partition. Dropping the scan aborts all owned tasks; Tokio releases their state
when cancellation is next scheduled. Cancelled `next` calls must be followed by
dropping the reader, as required by the shared reader contract. Already delivered
batches are never replayed. No source snapshot guarantee is added.

## Source ordering contract

`ScanPlan::ordering` describes permission for the entire selected input; it does
not select an emission policy. Constructors default to `PreserveGlobal`.
Providers may explicitly declare `Reorderable` after establishing that contract.
`require_ordering` adds an enclosing dataset or service restriction and cannot
relax a child's requirement. ATIF plans always retain global order.

Every record accessor and reader returns `RecordCapability`, whose `ordering`
field is mandatory. Neither `RecordCapability` nor `OrderRequirement` has a
default. `Reader::Capability` fixes the output type: record implementations must
return record capabilities, while byte/storage interfaces continue to return
`Capability`. Factories must implement `capability` explicitly, including custom
formats. The former `ScanPlanner::order_requirement()` default is removed.

CSV, JSON, XML, Parquet, raw WARC, individual documents, arXiv/MECA entries and
PMC inventory rows declare reorderable records. ATIF declares `PreserveGlobal`.
HF dataset scans explicitly retain global order because a Parquet export does not
establish the dataset's semantic ordering contract. Application-defined page sets
also explicitly preserve global order because their callback does not declare a
selection-wide reorderability guarantee.

FileSet and Directory always forward the supplied record accessor's ordering,
including whole-file planning without `with_file_planning()`. Native planning
flattens finer partitions under the same contract and budget. The first file alone
never grants permission. Later native plans must honor the accessor-wide contract,
including empty plans, as well as the fixed schema. Custom mixed accessors must
explicitly declare `PreserveGlobal` unless they can guarantee the entire selection
is reorderable. Partition discovery stays lazy and adds no snapshot guarantee.

Opened record readers retain their source's constraint. `ScanPlan::whole_input`
derives its ordering from the reader, and enclosing execution/emission layers
combine the source capability with the resolved plan before admission. A source
requirement cannot be weakened by requesting unordered output. Row-local generated
columns and type conversion preserve the inner capability's ordering.

## Execution and emission settings

Execution layers collect `ScanPlan::parallel_read` and `ScanPlan::emission` before
starting workers. `ParallelReadOptions` holds one resident-partition and prefetch
budget. Different parallel budgets on the same composed scan are rejected rather
than creating nested executors. Repeating the same budget is idempotent. Put these
layers around the complete dataset scan; native file planning rejects child
execution policies instead of silently discarding them during flattening.

`EmissionPolicy::Ordered` is the default. `Unordered` is admitted only for a
`Reorderable` plan; global order requirements cause an `InvalidInput` error before
workers start, including for empty and single-partition scans. Use `UnorderedEmissionLayer` for the public opt-in; custom planners can also set
the plan policy.

Unordered merging polls all resident receivers, returning ready batches without
waiting for an earlier partition. FIFO order within each partition is retained;
there is no guarantee of partition contiguity or a reproducible interleaving.
Polling rotates among ready receivers to avoid starving a partition. Completed
partitions retain their slots until their queues drain. Both policies share the
same worker, reservation-before-decode backpressure, schema checks, fail-fast
errors and drop cancellation. Queues bound batch counts, not total memory bytes.

## Opting into unordered emission

```rust,ignore
use std::num::NonZeroUsize;
use open_arrow_access::{Accessor, AccessorExt, layers::{ParallelReadLayer, UnorderedEmissionLayer}};

let source = files.with_file_planning()
    .layer(ParallelReadLayer::new(
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    ))
    .layer(UnorderedEmissionLayer::new());
let mut reader = source.open(request).await?;
while let Some(batch) = reader.next().await? {
    // Ready batches may arrive from any resident partition.
}
```

Swapping the two layers produces the same policy and one executor budget.
Put execution/emission layers around the complete dataset or service scan.
Unordered emission alone validates permission and uses the sequential plan reader;
it neither spawns tasks nor requires a Tokio runtime. It does not promise to
shuffle records. A `read_parallel` capability does not grant unordered permission.
ATIF, HF's conservative dataset contract, and unknown/custom sources reject the
request unless their source contract explicitly permits it. Repeated unordered
layers are idempotent. Do not derive row identities or generated values from batch
arrival order; use source identity and original positions.
