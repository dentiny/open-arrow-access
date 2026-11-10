use std::num::NonZeroUsize;

/// Input limits for file-internal decode partitions. These bound framed bytes,
/// not upstream chunk allocations, decoder state, or consumer-held Arrow batches.
#[derive(Clone, Copy, Debug)]
pub struct DecodePartitionOptions {
    /// Target serialized bytes per partition; one larger record may exceed this.
    pub target_bytes: NonZeroUsize,
    /// Reject an individual serialized record exceeding this limit.
    pub max_record_bytes: NonZeroUsize,
}

impl Default for DecodePartitionOptions {
    fn default() -> Self {
        Self {
            target_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            max_record_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        }
    }
}

#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) mod error;
#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) mod input;
