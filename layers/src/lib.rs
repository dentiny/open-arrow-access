//! Format-independent transformations following the shared `Layer` contract.

#[cfg(feature = "schema-evolution")]
mod batch;
#[cfg(feature = "coalesce-batches")]
mod coalesce_batches;
#[cfg(feature = "coalesce-batches")]
pub use coalesce_batches::{CoalesceBatchesAccessor, CoalesceBatchesLayer};

#[cfg(any(
    feature = "coalesce-batches",
    feature = "filter",
    feature = "sampling",
    feature = "schema-evolution",
    feature = "parallel-read",
    feature = "prefetch",
    feature = "source-columns"
))]
mod error;
#[cfg(any(feature = "schema-evolution", feature = "filter"))]
mod expression;
#[cfg(feature = "parallel-read")]
mod scan_policy;
#[cfg(any(
    feature = "filter",
    feature = "sampling",
    feature = "schema-evolution",
    feature = "parallel-read",
    feature = "source-columns"
))]
mod validation;
#[cfg(feature = "schema-evolution")]
mod value_lifetime;

#[cfg(feature = "prefetch")]
mod prefetch;
#[cfg(feature = "prefetch")]
pub use prefetch::{PrefetchAccessor, PrefetchLayer};

#[cfg(feature = "parallel-read")]
mod ordered_buffer;
#[cfg(feature = "parallel-read")]
mod parallel_read;
#[cfg(feature = "parallel-read")]
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

#[cfg(feature = "unordered-emission")]
mod unordered_emission;
#[cfg(feature = "unordered-emission")]
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};

#[cfg(feature = "limit")]
mod limit;
#[cfg(feature = "limit")]
pub use limit::{LimitAccessor, LimitLayer};

#[cfg(feature = "filter")]
mod filter;
#[cfg(feature = "filter")]
pub use filter::{FilterAccessor, FilterLayer, FilterRule};

#[cfg(feature = "sampling")]
mod sampling;
#[cfg(feature = "sampling")]
pub use sampling::{SamplingAccessor, SamplingLayer};

#[cfg(feature = "schema-evolution")]
mod schema_evolution;
#[cfg(feature = "schema-evolution")]
pub use schema_evolution::{
    ColumnEvolution, ColumnExpression, SchemaEvolutionAccessor, SchemaEvolutionLayer,
};

/// Read observability for byte and record accessors.
#[cfg(feature = "metrics")]
pub use open_arrow_access_core::metrics::{MetricsAccessor, MetricsLayer};

#[cfg(feature = "source-columns")]
mod source_columns;
#[cfg(feature = "source-columns")]
pub use source_columns::{SourceColumnBatch, SourceColumns, SourceColumnsAccessor};

#[cfg(feature = "source-positions")]
pub use source_columns::{SourcePositionAccessor, SourcePositionLayer};

#[cfg(feature = "missing-object")]
mod missing_object;
#[cfg(feature = "missing-object")]
pub use missing_object::{MissingObjectAccessor, MissingObjectLayer, MissingObjectPolicy};

#[cfg(feature = "attachment-fallback")]
mod attachment_fallback;
#[cfg(feature = "attachment-fallback")]
pub use attachment_fallback::{AttachmentFallbackAccessor, AttachmentFallbackLayer};

#[cfg(feature = "whole-object")]
mod whole_object;
#[cfg(feature = "whole-object")]
pub use whole_object::{WholeObjectAccessor, WholeObjectLayer};
