use open_arrow_access_core::{ByteMetadata, ByteReader, Result, SeekableInput, into_async_seek};

use std::num::NonZeroUsize;

#[cfg(feature = "zip")]
pub(crate) const DEFAULT_MAX_ENTRY_BYTES: u64 = 1024 * 1024 * 1024;

/// One opened archive source. Clones share storage with independent cursors.
#[derive(Clone)]
pub(crate) struct ArchiveSource {
    pub(crate) input: SeekableInput,
    pub(crate) metadata: ByteMetadata,
}

impl ArchiveSource {
    pub(crate) fn new(
        input: Box<dyn ByteReader>,
        operation: &'static str,
        chunk_size: NonZeroUsize,
    ) -> Result<Self> {
        let metadata = input.metadata().clone();
        let input = into_async_seek(input, chunk_size, operation)?;
        Ok(Self { input, metadata })
    }
}
