use crate::FileAccessor;

use super::WarcFormat;

/// Parse owned physical WARC records through the shared file accessor.
/// Metadata is the input locator; output uses native records rather than Arrow batches.
pub type WarcAccessor<B> = FileAccessor<B, WarcFormat>;

impl<B> FileAccessor<B, WarcFormat> {
    pub fn new(inner: B) -> Self {
        Self::from_format(inner, WarcFormat)
    }
}
