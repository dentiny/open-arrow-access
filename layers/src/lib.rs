//! Format-independent transformations following the shared `Layer` contract.

mod error;
mod type_conversion;
mod validation;

#[cfg(feature = "generated-columns")]
mod generated_columns;
#[cfg(feature = "generated-columns")]
pub use generated_columns::{GeneratedColumnSpec, GeneratedColumnsAccessor, GeneratedColumnsLayer};

pub use type_conversion::{ColumnTypeSpec, TypeConversionAccessor, TypeConversionLayer};

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};
