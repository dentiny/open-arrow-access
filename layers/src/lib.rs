//! Format-independent transformations following the shared `Layer` contract.

mod batch;
mod error;
mod expression;
mod source_projection;
mod validation;
mod value_lifetime;

mod generated_columns;
pub use generated_columns::{GeneratedColumnSpec, GeneratedColumnsAccessor, GeneratedColumnsLayer};

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

mod unordered_emission;
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};

mod sampling;
pub use sampling::{SamplingAccessor, SamplingLayer};

mod schema_evolution;
pub use schema_evolution::{ColumnEvolution, SchemaEvolutionAccessor, SchemaEvolutionLayer};
