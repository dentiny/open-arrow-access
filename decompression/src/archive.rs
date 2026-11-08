use open_arrow_access_core::{ByteMetadata, ByteReader, Result, SeekableInput, into_async_seek};

use crate::layer::DEFAULT_CHUNK_SIZE;

pub(crate) const DEFAULT_MAX_ENTRY_BYTES: u64 = 1024 * 1024 * 1024;

/// One opened archive source. Clones share storage with independent cursors.
#[derive(Clone)]
pub(crate) struct ArchiveSource {
    pub(crate) input: SeekableInput,
    pub(crate) metadata: ByteMetadata,
}

impl ArchiveSource {
    pub(crate) fn new(input: Box<dyn ByteReader>, operation: &'static str) -> Result<Self> {
        let metadata = input.metadata().clone();
        let input = into_async_seek(input, DEFAULT_CHUNK_SIZE, operation)?;
        Ok(Self { input, metadata })
    }
}
