//! Format-independent transformations following the shared `Layer` contract.

mod type_conversion;

pub use type_conversion::{ColumnTypeSpec, TypeConversionAccessor, TypeConversionLayer};
