# Proposal: scan statistics for ingestion planning

Status: proposed. This document specifies a contract and rollout plan; it does not implement a public API.

## Motivation and ownership

Ingestion applications need metadata before consuming records so that a sink can estimate output file count and choose an initial writer count. Encoded input size alone is insufficient: projection, compression, filtering, generated columns, and downloaded blobs can change output volume.

Access reports source facts and estimates. The sink owns target file size, encoding, concurrency, and file rotation. Applications adapt metadata between the libraries; neither core depends on the other.

Companion: [sink write planning](https://github.com/dentiny/open-arrow-sink/blob/docs/metadata-driven-ingestion-design/docs/design/metadata-driven-ingestion.md).

## Existing contracts

`core/src/statistics.rs` defines the optional `StatisticsProvider` extension. `ScanStatistics` contains `row_count` and `source_bytes`, each using `Statistic::Exact`, `Estimated`, or `Unknown`. Source bytes describe the encoded input exposed by the byte accessor, not decoded Arrow memory or projected bytes fetched.

Keep statistics separate from `RecordMetadata`, Arrow schema metadata, and ordinary `Accessor::open`. Existing statistics and prepared sources do not pin source versions. `ScanPlanner` exposes lazy work; it does not itself execute it.

## Proposed metadata

```rust,ignore
pub struct ScanStatistics {
    pub row_count: Statistic<u64>,
    pub source_bytes: Statistic<u64>,
    pub logical_bytes: Statistic<u64>,
}
```

This is an API sketch. Adding a public field needs a compatibility review and an appropriate release because external struct literals can break.

| Field | Scope | Use |
| --- | --- | --- |
| row_count | Rows after selection and row-changing access layers | Cardinality planning |
| source_bytes | Selected encoded inputs in the exposed byte representation | Input volume and I/O hints |
| logical_bytes | Logical Arrow output payload after projection and access layers | Input to a sink encoding estimate |

Unknown is distinct from exact zero. Each field retains its own accuracy. Inspection may use cheap format metadata or provider estimates, but must not consume/decode selected rows merely to count them. Unavailable or prohibitively expensive values remain Unknown.

### Logical byte accounting

Finalize one versioned accounting rule before implementation. The proposed first rule counts logical Arrow buffer content: values, offsets, and validity buffers when present, including nested children. Exclude allocator spare capacity, object headers, decoder state, and transport buffers. Charge slices for referenced logical ranges rather than entire backing allocations. Count dictionary values once per logical batch; do not assume cross-batch dictionary reuse.

This quantity is not a resident-memory bound. Batch boundaries, dictionary layout, and decoder representation can affect it. Format metadata normally yields Estimated values. Exact requires establishing this defined quantity for the selected output; a Parquet uncompressed-size total is not exact logical Arrow bytes.

## Selection and layer propagation

Statistics describe the same typed selection and effective projection as the scan. Reusing them across changed selections or transformations requires updating their scope.

| Operation | Row count | Logical bytes |
| --- | --- | --- |
| Projection | Preserve | Recompute or Unknown |
| Row-preserving transforms | Preserve | Recompute, estimate, or Unknown |
| Generated columns / casts | Preserve if row preserving | Estimate or Unknown |
| Filter / sampling / limit | Derive by proven rule, estimate, or Unknown | Derive, estimate, or Unknown |
| Parallel read / emission policy | Preserve | Preserve |
| Explicit duplicate file selections | Count each occurrence | Count each occurrence |
| Dataset concatenation | Checked sum | Checked sum |

A sum containing Unknown is Unknown; all Exact inputs yield Exact; otherwise it is Estimated. Overflow must not wrap or saturate into a misleading exact value: return Unknown or a documented error. Unsupported layers conservatively invalidate statistics. Source bytes remain input volume, not post-filter output size.

Exact zero rows can establish emptiness only for a compatible pipeline. Zero-column projection may retain nonzero rows with zero logical payload; sinks still have row/file overhead. Zero logical bytes alone does not prove empty input.

URL strings do not account for downloaded blob payload. Applications materializing blobs after access must replace or invalidate sink size estimates. Separate provider payload estimates are a future extension.

## Availability and cost

Keep `StatisticsProvider` optional. Ordinary reads must not gain mandatory listing, footer reads, sampling, or full scans. Providers document metadata I/O cost and byte representation.

Applications can inspect typed accessors before erasing them into `PreparedSource`. Preserving optional statistics through that erasure is a follow-up API decision; preparation must not silently start expensive inspection or promise metadata caching.

Do not enumerate all lazy partitions merely to obtain a count. A single sequential reader can feed multiple sink writers.

## Consistency and ordering

Exact means exact for the observed input during collection, not a snapshot of a later scan. File-count and concurrency decisions are hints. Source changes may alter layout/performance, but never justify dropping, replaying, or truncating records.

This proposal adds no snapshot binding. A future snapshot API must carry actual provider versions through planning and opening, not merely attach an informational token.

Statistics grant no reordering permission. Existing source ordering and explicit emission policies remain authoritative.

## Rollout and validation

1. Finalize logical byte accounting and compatibility strategy.
2. Add the optional field with Unknown as the conservative fallback.
3. Add suitable format estimates and checked dataset aggregation; audit layer propagation.
4. Document application mapping to sink-owned input metadata.

Implementation tests cover Unknown versus zero, mixed-accuracy sums, overflow, duplicates, projection including zero-column output, transformations, and changed sources. Assert that inspection does not consume records. Benchmark metadata latency separately from ingestion throughput and memory.

## Deferred work

Automatic sampling, snapshot binding, per-partition size annotations, external blob payload statistics, and a shared metadata crate are outside the first implementation.
