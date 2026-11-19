use crate::{
    EmissionPolicy,
    layers::{CoalesceBatchesLayer, ParallelReadLayer, PrefetchLayer, SchemaEvolutionLayer},
};

/// Scan-wide execution settings. Layer configurations retain their complete public APIs.
/// Byte I/O, HTTP limits, retries and codecs belong to the supplied source/client.
#[derive(Clone, Debug)]
pub struct CrawlOptions {
    pub parallel_read: ParallelReadLayer,
    /// Defaults to unordered; sources requiring global order reject it.
    pub emission: EmissionPolicy,
    /// Row-local transforms execute in partition workers, before queue admission.
    pub evolution: SchemaEvolutionLayer,
    /// Optional scan-wide coalescing after merging; useful for document sources.
    pub coalesce: Option<CoalesceBatchesLayer>,
    /// Optional scan-wide read-ahead after coalescing, with its own explicit budget.
    pub prefetch: Option<PrefetchLayer>,
}

impl CrawlOptions {
    /// Require an explicit concurrency/prefetch budget rather than choosing one
    /// HTTP request rate for both small metadata objects and large streaming files.
    pub fn new(parallel_read: ParallelReadLayer) -> Self {
        Self {
            parallel_read,
            emission: EmissionPolicy::Unordered,
            evolution: SchemaEvolutionLayer::default(),
            coalesce: None,
            prefetch: None,
        }
    }
}
