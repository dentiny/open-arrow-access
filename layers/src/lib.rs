//! Format-independent transformations following the shared `Layer` contract.

mod batch;
mod error;
mod expression;
mod scan_policy;
mod validation;
mod value_lifetime;

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

mod unordered_emission;
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};

mod sampling;
pub use sampling::{SamplingAccessor, SamplingLayer};

mod schema_evolution;
pub use schema_evolution::{
    ColumnEvolution, ColumnExpression, SchemaEvolutionAccessor, SchemaEvolutionLayer,
};
