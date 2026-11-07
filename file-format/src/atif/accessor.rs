use super::AtifFormat;
use crate::FileAccessor;

/// Read ATIF v1.0–v1.8 with the fixed `atif-arrow` schema, one trajectory per row.
///
/// Consumes sequential bytes from any byte accessor. Whole documents are buffered;
/// batch size limits rows, not bytes. Projection selects top-level output columns
/// after validation and conversion of the entire trajectory.
pub type AtifAccessor<B> = FileAccessor<B, AtifFormat>;

impl<B> FileAccessor<B, AtifFormat> {
    pub fn new(inner: B, format: AtifFormat) -> Self {
        Self::from_format(inner, format)
    }
}
