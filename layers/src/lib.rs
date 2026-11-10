//! Format-independent transformations following the shared `Layer` contract.

mod type_conversion;
mod validation;

pub use type_conversion::{ColumnTypeSpec, TypeConversionAccessor, TypeConversionLayer};
