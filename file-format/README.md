### Parquet row-group planning

`ParquetAccessor` implements `ScanPlanner` when its byte accessor is cloneable
(use `Arc` for a shared non-cloneable accessor). Planning reads the footer once,
fixes the projected schema, and creates one independently opened partition per
row group. Install `ParallelReadLayer` to execute these partitions while preserving
row order. Normal `open` remains sequential. Range reads and known input length
are required after byte layers; ordinary sequential gzip input is rejected.
Known input length/version changes are rejected when partitions open; absent a
storage version, this does not provide a snapshot of a mutable object.

### JSON object partitions

`JsonAccessor::plan_scan` frames complete whitespace-separated JSON objects,
including JSONL and pretty-printed objects, and emits owned decode partitions
lazily. Each partition uses an independent Arrow decoder with the fixed projected
schema. Framing uses serde_json RawValue sequentially; Arrow decoding runs in
executor workers. Incomplete records are rescanned when more input arrives, so
large records delivered in tiny chunks can increase framing cost.
This works over sequential decompression and does not require range access.

Use `FileAccessor::with_partition_options` to configure a soft chunk-byte target
(default 4 MiB); the row target is the request's batch size. A complete record can
exceed the byte target and is never rejected for its size. The resident task window
bounds the number of retained chunks, with one pending lookahead record plus the
upstream input buffer. This is not a cap on input bytes, Arrow allocations, or decoder state. Scalar/array top-level inputs
remain unsupported. Output stays ordered and batch boundaries may change.

### ATIF trajectory partitions

ATIF JSON Lines plans frame bounded groups of complete nonblank trajectories,
assign global source record indices before scheduling, and decode each group
independently. Output remains globally ordered, including raw_json, source URI,
projection, and zero-column row counts. The original line endings are retained.
Single-document ATIF JSON remains a whole-input partition. Framing can consume
sequential decoded bytes; it does not require random reads or parallel gzip.
