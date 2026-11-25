use std::num::NonZeroUsize;

use crate::{
    EmissionPolicy,
    layers::{CoalesceBatchesLayer, ParallelReadLayer, PrefetchLayer},
};

#[cfg(feature = "layer-schema-evolution")]
use crate::layers::SchemaEvolutionLayer;

/// Scan-wide execution settings. Layer configurations retain their complete public APIs.
/// Byte I/O, HTTP limits, retries and codecs belong to the supplied source/client.
/// See the [tuning workflow](https://github.com/dentiny/open-arrow-access/blob/main/docs/crawl.md#tuning-parameters-by-pipeline-stage)
/// for the stage controlled by each parameter and its throughput/memory tradeoffs.
#[derive(Clone, Debug)]
pub struct CrawlOptions {
    /// Resident partition/file concurrency, native batch prefetch and per-file
    /// read-ahead byte budget, shared by the crawl's single executor.
    /// Defaults to 4 resident partitions, 2 queued native batches per partition
    /// and 8 MiB of read-ahead per streaming file.
    pub parallel_read: ParallelReadLayer,
    /// Defaults to unordered; sources requiring global order reject it.
    pub emission: EmissionPolicy,
    /// Optional row-local transforms; the default leaves schema and values unchanged.
    /// Configured transforms execute in partition workers, before queue admission.
    #[cfg(feature = "layer-schema-evolution")]
    pub evolution: SchemaEvolutionLayer,
    /// Optional scan-wide coalescing after merging; useful for document sources.
    pub coalesce: Option<CoalesceBatchesLayer>,
    /// Optional scan-wide read-ahead after coalescing, with its own explicit budget.
    pub prefetch: Option<PrefetchLayer>,
}

impl Default for CrawlOptions {
    fn default() -> Self {
        Self {
            parallel_read: ParallelReadLayer::new(
                NonZeroUsize::new(4).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            ),
            emission: EmissionPolicy::Unordered,
            #[cfg(feature = "layer-schema-evolution")]
            evolution: SchemaEvolutionLayer::default(),
            coalesce: None,
            prefetch: None,
        }
    }
}

impl CrawlOptions {
    /// Replace the concurrency, native prefetch and streaming read-ahead settings.
    pub fn with_parallel_read(mut self, parallel_read: ParallelReadLayer) -> Self {
        self.parallel_read = parallel_read;
        self
    }

    /// Select ordered or unordered emission without weakening source requirements.
    pub fn with_emission(mut self, emission: EmissionPolicy) -> Self {
        self.emission = emission;
        self
    }

    /// Configure row-local schema and value transformations.
    #[cfg(feature = "layer-schema-evolution")]
    pub fn with_evolution(mut self, evolution: SchemaEvolutionLayer) -> Self {
        self.evolution = evolution;
        self
    }

    /// Enable scan-wide coalescing with the supplied row and byte budgets.
    pub fn with_coalesce(mut self, coalesce: CoalesceBatchesLayer) -> Self {
        self.coalesce = Some(coalesce);
        self
    }

    /// Enable merged-stream prefetch with the supplied row and byte budgets.
    pub fn with_prefetch(mut self, prefetch: PrefetchLayer) -> Self {
        self.prefetch = Some(prefetch);
        self
    }
}
