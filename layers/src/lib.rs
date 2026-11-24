//! Format-independent transformations following the shared `Layer` contract.

mod batch;
mod coalesce_batches;
pub use coalesce_batches::{CoalesceBatchesAccessor, CoalesceBatchesLayer};

mod error;
mod expression;
mod scan_policy;
mod validation;
mod value_lifetime;

mod prefetch;
pub use prefetch::{PrefetchAccessor, PrefetchLayer};

mod ordered_buffer;
mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

mod unordered_emission;
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};

mod limit;
pub use limit::{LimitAccessor, LimitLayer};

mod filter;
pub use filter::{FilterAccessor, FilterLayer};

mod sampling;
pub use sampling::{SamplingAccessor, SamplingLayer};

mod schema_evolution;
pub use schema_evolution::{
    ColumnEvolution, ColumnExpression, SchemaEvolutionAccessor, SchemaEvolutionLayer,
};

/// Read observability for byte and record accessors.
pub use open_arrow_access_core::metrics::{MetricsAccessor, MetricsLayer};

mod source_columns;
pub use source_columns::{SourceColumnBatch, SourceColumns, SourceColumnsAccessor};

#[cfg(feature = "source-positions")]
pub use source_columns::{SourcePositionAccessor, SourcePositionLayer};

mod missing_object;
pub use missing_object::{MissingObjectAccessor, MissingObjectLayer, MissingObjectPolicy};

mod attachment_fallback;
pub use attachment_fallback::{AttachmentFallbackAccessor, AttachmentFallbackLayer};
