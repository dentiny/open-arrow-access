use super::ParquetFormat;
use crate::FileAccessor;

/// Read one Parquet file. The input needs range reads and a known byte length.
pub type ParquetAccessor<B> = FileAccessor<B, ParquetFormat>;

impl<B> FileAccessor<B, ParquetFormat> {
    pub fn new(inner: B) -> Self {
        Self::from_format(inner, ParquetFormat)
    }
}
