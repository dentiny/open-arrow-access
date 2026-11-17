//! Format-independent transformations following the shared `Layer` contract.

mod batch;
mod error;
mod expression;
mod scan_policy;
mod validation;
mod value_lifetime;

mod prefetch;
pub use prefetch::{PrefetchAccessor, PrefetchLayer};

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

mod unordered_emission;
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};

mod limit;
pub use limit::{LimitAccessor, LimitLayer};

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
