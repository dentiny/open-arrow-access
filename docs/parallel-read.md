# Ordered parallel reading

`ParallelReadLayer` runs independent partitions on Tokio tasks and emits every
partition in declared order. It does not change record order or enable unordered
emission. FileSet preserves explicit duplicates and selection order; Directory
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
Ordinary `open` remains sequential. Planning opens only the first selected file
to fix the schema; subsequent files open in workers. All partition metadata and
batches must match that schema, including metadata. Empty file sets remain errors.

With P resident partitions and Q queue slots per partition, at most P readers
are active and P * Q batches are queued. A worker reserves queue capacity before
calling `next`. A finished worker retains its partition slot until its queue has
drained; a slow first file cannot cause unlimited later files to accumulate.
The consumer can hold additional batches. Decoder allocations, individual batch
sizes and compressed input buffers are not bounded by these queue limits.
Storage's per-reader I/O limit remains separate: P readers may each use their
configured I/O concurrency. This version does not impose a backend-global cap.

Each worker owns its reader and codec state, so file-level parallelism composes
with a decompression accessor installed below the format accessor. Decoded bytes
within each file retain their original order. Parquet row-group planning and
single-file JSONL parallel decode are subsequent milestones, not provided here.

Workers begin on first consumption and require an active Tokio runtime. On a
multithreaded runtime decoder work can run on separate runtime threads; a single
thread runtime overlaps asynchronous waits but does not provide CPU parallelism.
Errors and panics terminate the scan, including failures behind a blocked first
partition. Dropping the scan aborts all owned tasks; Tokio releases their state
when cancellation is next scheduled. Cancelled `next` calls must be followed by
dropping the reader, as required by the shared reader contract. Already delivered
batches are never replayed. No source snapshot guarantee is added.
