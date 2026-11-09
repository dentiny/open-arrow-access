use super::AtifFormat;
use crate::FileAccessor;

/// Read ATIF v1.0–v1.8 with the fixed `atif-arrow` schema, one trajectory per row.
///
/// Buffers whole trajectories; batch size limits rows, not bytes. Projection
/// selects top-level columns after full validation and conversion.
pub type AtifAccessor<B> = FileAccessor<B, AtifFormat>;

impl<B> FileAccessor<B, AtifFormat> {
    pub fn new(inner: B, format: AtifFormat) -> Self {
        Self::from_format(inner, format)
    }
}
