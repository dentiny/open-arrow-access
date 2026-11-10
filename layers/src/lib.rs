//! Format-independent transformations following the shared `Layer` contract.

mod error;
mod type_conversion;
mod validation;

pub use type_conversion::{ColumnTypeSpec, TypeConversionAccessor, TypeConversionLayer};

mod parallel_read;
pub use parallel_read::{ParallelReadAccessor, ParallelReadLayer};
