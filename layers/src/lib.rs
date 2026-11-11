//! Format-independent transformations following the shared `Layer` contract.

mod error;
mod type_conversion;
mod validation;

mod generated_columns;
pub use generated_columns::{GeneratedColumnSpec, GeneratedColumnsAccessor, GeneratedColumnsLayer};

pub use type_conversion::{ColumnTypeSpec, TypeConversionAccessor, TypeConversionLayer};

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};

mod unordered_emission;
pub use unordered_emission::{UnorderedEmissionAccessor, UnorderedEmissionLayer};
