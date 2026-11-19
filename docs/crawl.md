# Integrated crawls

`open_arrow_access::crawl` supplies one execution entry point for existing service
and dataset planners and a discovery-only interface for new services. It is always
available; enable the formats, services and codecs your source uses.

```text
request → service discovery / manifest / streaming listing → native ScanPlan
        → partition workers: read + decode + row-local evolution
        → bounded queues → ordered/unordered merge → optional coalesce/prefetch
        → RecordReader
```

## Existing sources and layer configuration

Pass an existing `ScanPlanner` to `CrawlAccessor::new(source, options)`. For example,
`PmcDatasetAccessor`, `PmcInventoryAccessor`, `CommonCrawlWarcAccessor` and
`CommonCrawlSegmentAccessor` retain their existing discovery and decoder behavior.
Call `with_file_planning()` on Common Crawl accessors to expose native planning.
`CrawlAccessor::files(format, options)` constructs a file set with native planning;
`CrawlAccessor::directory(discovery, format, options)` does the same for directories
and globs. Directory discovery still collects and sorts names; streaming service
planners remain streaming.

```rust,ignore
use std::num::NonZeroUsize;
use arrow_schema::DataType;
use open_arrow_access::{
    Accessor, ReadRequest,
    crawl::{CrawlAccessor, CrawlOptions},
    layers::{ColumnEvolution, CoalesceBatchesLayer, ParallelReadLayer,
             PrefetchLayer, SchemaEvolutionLayer},
};

let parallel_read = ParallelReadLayer::new(
    NonZeroUsize::new(64).unwrap(), // resident partitions/files
    NonZeroUsize::new(2).unwrap(),  // native partition batch queue
).with_read_ahead_bytes(NonZeroUsize::new(8 * 1024 * 1024).unwrap());
let options = CrawlOptions {
    evolution: SchemaEvolutionLayer::try_new(vec![
        ColumnEvolution::rename_column("paperId", "paper_id"),
        ColumnEvolution::change_data_type("year", DataType::Int64),
    ])?,
    coalesce: Some(CoalesceBatchesLayer::rows(NonZeroUsize::new(1024).unwrap())
        .with_max_estimated_bytes(NonZeroUsize::new(4 * 1024 * 1024).unwrap())),
    prefetch: Some(PrefetchLayer::estimated_bytes(
        NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
    ).with_max_rows(NonZeroUsize::new(8192).unwrap())),
    ..CrawlOptions::new(parallel_read)
};
let crawl = CrawlAccessor::new(source, options);
let mut reader = crawl.open(request).await?;
while let Some(batch) = reader.next().await? { /* consume batch */ }
```

These values illustrate the independent budgets, not a production recommendation.
Every field accepts the existing layer configuration directly, including all its
builder settings. `CrawlOptions::new(parallel_read)` requires an explicit worker
budget, defaults to unordered emission and identity evolution, and leaves
coalescing/prefetch disabled. Configure `emission: EmissionPolicy::Ordered` for
sources that require global ordering or for reproducible sampling.

Request options retain batch size, projection and byte I/O settings. Projections
use evolved names. Supplying `evolution.with_source_schema(full_schema)` pushes
only required source columns down without an additional schema-discovery open.
Without a supplied schema, namespace-changing evolution widens projection using
the existing evolution rules. Identity evolution delegates planning directly.

## Service discovery

Implement `CrawlResolver` when a service needs to pin a release/revision, inspect
shard metadata and resolve files before constructing its source:

```rust,ignore
use open_arrow_access::{
    Accessor, ByteAccessor, ReadRequest, RecordCapability, Result, ScanPlanner, async_trait,
    crawl::{CrawlAccessor, CrawlResolver, ResolvedCrawl},
    dataset::{file_set::FileSetAccessor, manifest::PathsManifest},
};
use std::sync::Arc;

struct ManifestResolver<B, F> {
    raw_manifests: B,
    format: Arc<F>, // already bound to storage, codecs and transport policy
}

#[async_trait]
impl<B, F> CrawlResolver for ManifestResolver<B, F>
where
    B: ByteAccessor,
    F: ScanPlanner<Request = ReadRequest> + 'static,
{
    type Request = ReadRequest; // object identifies a paths manifest
    type Source = FileSetAccessor<Arc<F>>;

    fn capability(&self) -> RecordCapability {
        self.format.capability()
    }

    async fn resolve(&self, request: ReadRequest) -> Result<ResolvedCrawl<Self::Source>> {
        // Manifest selection/base/duplicate/format policies can be configured here.
        // This opens raw manifest bytes; PathsManifest owns suffix decompression.
        let files = PathsManifest::new(request.object)
            .with_byte_options(request.byte_options)
            .resolve(&self.raw_manifests).await?;
        Ok(ResolvedCrawl {
            accessor: FileSetAccessor::new(self.format.clone()).with_file_planning(),
            request: files,
        })
    }
}

let crawl = CrawlAccessor::from_resolver(resolver, options);
let reader = crawl.open(ReadRequest::new("release/shards.paths.gz")).await?;
```

The resolver owns provider-specific authentication, snapshot pinning, selection
and manifest validation. `ResolvedCrawl` carries both the selected request and the
accessor bound to that snapshot, including signed URLs and transport context.
The integration forwards common read and exposed byte options to the selected
request, even if the resolver constructed it with defaults. The resolver must
also forward byte options to any metadata I/O it performs during discovery.

Each `open` resolves once and plans once; it does not call `metadata` first or
reopen the preview file. Separate metadata/open operations resolve separately.
For a reusable fixed selection, resolve once and pass the resolved accessor and
request to `SelectionAccessor`, then wrap that with `CrawlAccessor::new`.
The resolver's ordering requirement combines with both the returned accessor and
plan; no layer may weaken a global requirement. Existing HF dataset and PageSet
contracts therefore require ordered emission. Token-dependent pagination remains
sequential inside each independent continuation chain.

## Performance boundaries

- There is one existing parallel executor, no extra file worker pool. Native
  Parquet partitions survive; JSON/CSV/WARC retain their streaming-file planning.
  Discovery and partition futures stay lazy. `FileSetAccessor` keeps its preview
  plan/inference sample for execution.
- Row-local evolution compiles once per scan and shares an immutable plan across
  workers. No SQL planning or schema-name lookup is added to each row. Renames
  share arrays; casts/computed columns allocate their outputs as before. Transform
  output is admitted to the existing bounded partition queues.
- A slow partition does not block other ready batches in unordered mode.
  Coalescing then combines tiny document batches before a downstream sink. It has
  no flush timer, so large row targets can increase first-batch latency.
- The file read-ahead byte limit, native partition batch queue, optional merged
  prefetch budget, decoder buffers and caller-held batches are distinct. Byte
  estimates are soft budgets, not a process RSS cap. Completed queues retain their
  resident slots until drained. Errors and dropping readers use the existing
  executor's cancellation behavior.
- No operator/client is recreated and no byte chunk size or HTTP concurrency is
  guessed. Preserve PMC's small-object sequential GET path, WARC's sequential
  download/BinaryView defaults, and caller-supplied shared HTTP rate/backoff policy.
  File concurrency is separate from each reader's I/O and HTTP limits; increasing
  all three can amplify requests rather than improve completed throughput.
- Source statistics and sink writer planning remain application concerns. If the
  source implements `StatisticsProvider`, retain an `Arc` to it alongside the
  crawl and inspect the same selection before execution. Do not infer body sizes
  from metadata bytes or let unknown output size silently force a single writer.
- Sampling and limits stay outside this execution boundary. Ordered emission is
  needed for repeatable prefix/seeded selections. This accessor adds no retry,
  checkpoint or commit semantics; restarting a failed crawl does not imply that
  its previous output can safely be skipped.

## Differing schemas across files

A scan must have one fixed output schema. `options.evolution` runs after the
source's own planning/schema checks, so it cannot repair a file-set conflict that
already failed during planning. For string/numeric differences across shards,
apply normalization to the single-file format **before** building the file set:

```rust,ignore
let normalized = format.layer(normalization);
let files = FileSetAccessor::new(normalized).with_file_planning();
let crawl = CrawlAccessor::new(files, options);
```

`SchemaEvolutionLayer` now retains `ScanPlanner`, so this placement keeps native
partitions. Inferred per-file schemas may require separate normalization plans;
prefer an explicit format schema where practical. Use union-by-name only for
compatible types/missing fields; it retains its existing all-file preflight and
sample-memory costs. Invalid values/overflows still fail rather than silently
turning into nulls; use an explicit expression for a different conversion policy.
