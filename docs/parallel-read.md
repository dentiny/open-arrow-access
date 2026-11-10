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
Ordinary `open` remains sequential. By default, file-set planning opens the first
selected file to fix the schema and creates one partition per file. Empty file
sets remain errors.

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
metadata. Format-specific discovery may retain an input reader, framing state
and buffers separately from partition workers.

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
