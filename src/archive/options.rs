use std::{num::NonZeroUsize, sync::Arc};

use crate::{
    OuterCompression, Result,
    decompression::tar::{TarEntry, TarLimits},
};

type EntryFilter = dyn Fn(&TarEntry) -> Result<bool> + Send + Sync;

/// Interpretation of the bytes after outer decompression. Selection is explicit:
/// a gzip object can contain either a TAR archive or one ordinary file.
#[derive(Clone, Debug)]
pub enum ArchiveFormat {
    /// Traverse one TAR archive sequentially; nested archives stay binary cells.
    Tar,
    /// Emit one file using this opaque name, including when its body is empty.
    Single { name: Vec<u8> },
}

/// Package format, resource limits and member selection shared by independent opens.
#[derive(Clone)]
pub struct ArchiveOptions {
    /// Explicit container interpretation after decompression; no magic-byte sniffing.
    pub format: ArchiveFormat,
    /// Defaults to suffix-based decoding through `AutoDecompressionLayer`.
    /// Select `None` when the supplied byte accessor already decodes compression.
    pub outer_compression: OuterCompression,
    /// TAR header/member limits; `max_entry_bytes` also bounds a single-file body.
    pub limits: TarLimits,
    /// Soft selected-payload target, independent of the request's row-count target.
    /// A complete member can exceed this budget, up to `limits.max_entry_bytes`.
    pub target_bytes: NonZeroUsize,
    pub(super) filter: Arc<EntryFilter>,
}

impl ArchiveOptions {
    pub fn new(format: ArchiveFormat) -> Self {
        Self {
            format,
            outer_compression: OuterCompression::Auto,
            limits: TarLimits::default(),
            target_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            filter: Arc::new(|_| Ok(true)),
        }
    }

    /// Select regular TAR members from their headers, before buffering bodies.
    /// Directories are skipped first. Rejected bodies are still consumed in
    /// bounded chunks to reach later headers and validate the decoder through EOF.
    /// The callback can run concurrently across independent readers; errors abort
    /// that reader. Single-file format always emits its explicitly selected file.
    pub fn with_entry_filter(
        mut self,
        filter: impl Fn(&TarEntry) -> Result<bool> + Send + Sync + 'static,
    ) -> Self {
        self.filter = Arc::new(filter);
        self
    }
}
