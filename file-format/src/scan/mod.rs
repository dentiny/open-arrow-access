use std::num::NonZeroUsize;

/// Soft chunking targets for file-internal decode partitions. Complete records
/// are never rejected for exceeding the target; this is not a memory limit.
#[derive(Clone, Copy, Debug)]
pub struct DecodePartitionOptions {
    /// Target serialized bytes per partition; one larger record may exceed this.
    pub target_bytes: NonZeroUsize,
}

impl Default for DecodePartitionOptions {
    fn default() -> Self {
        Self {
            target_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
        }
    }
}

#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) mod error;
#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) mod input;
