//! Bounded inspection and selected-entry reads of uncompressed TAR archives.

mod error;
mod stream;
pub use stream::TarStream;
mod validation;

use std::{num::NonZeroUsize, sync::Arc};

use futures::stream::try_unfold;
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Result, SeekableInput, StreamReader, into_async_seek,
};

use crate::layer::DEFAULT_CHUNK_SIZE;
use error::{decode, missing_entry, unsupported};
use validation::{BLOCK_SIZE, entry_end, parse_entry};

const DEFAULT_MAX_ENTRIES: usize = 65_536;
const DEFAULT_MAX_ENTRY_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct TarLimits {
    pub max_entries: usize,
    pub max_entry_bytes: u64,
}

impl Default for TarLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_ENTRIES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
        }
    }
}

/// Original entry names are metadata, never paths extracted to the filesystem.
/// Duplicate names retain separate indices; directories cannot be opened as files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TarEntry {
    pub index: usize,
    pub name: Vec<u8>,
    pub size: u64,
    pub is_file: bool,
}

/// One opened source and its cached entry headers. Inspection reads header blocks
/// and skips bodies using exact ranges; there is one header request per entry.
/// Supports regular GNU/USTAR files and directories. Extension headers, sparse
/// files and links are explicitly unsupported. Compressed TAR requires a separate
/// ranged representation; a sequential gzip decoder cannot supply this input.
#[derive(Clone)]
pub struct TarArchive {
    input: SeekableInput,
    metadata: ByteMetadata,
    entries: Arc<[(TarEntry, u64)]>,
}

impl TarArchive {
    pub async fn open(input: Box<dyn ByteReader>, limits: TarLimits) -> Result<Self> {
        let metadata = input.metadata().clone();
        let input = into_async_seek(input, DEFAULT_CHUNK_SIZE, "open_tar")?;
        let mut offset = 0;
        let mut entries = Vec::new();
        loop {
            if input.content_length().saturating_sub(offset) < BLOCK_SIZE {
                return Err(decode("TAR archive has no complete end marker"));
            }
            let bytes = input.read_range(offset..offset + BLOCK_SIZE).await?;
            if bytes.iter().all(|byte| *byte == 0) {
                if input.content_length().saturating_sub(offset) < 2 * BLOCK_SIZE
                    || !input
                        .read_range(offset + BLOCK_SIZE..offset + 2 * BLOCK_SIZE)
                        .await?
                        .iter()
                        .all(|byte| *byte == 0)
                {
                    return Err(decode("TAR archive requires two zero end blocks"));
                }
                break;
            }
            let entry = parse_entry(&bytes, entries.len(), &limits)?;
            let start = offset + BLOCK_SIZE;
            offset = entry_end(start, entry.size, input.content_length())?;
            entries.push((entry, start));
        }
        Ok(Self {
            input,
            metadata,
            entries: entries.into(),
        })
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = &TarEntry> {
        self.entries.iter().map(|(entry, _)| entry)
    }

    /// Create a bounded sequential reader without reopening storage or reparsing
    /// headers. Each reader owns its cursor and can outlive this archive.
    pub fn open_entry(
        &self,
        index: usize,
        chunk_size: NonZeroUsize,
    ) -> Result<Box<dyn ByteReader>> {
        let (entry, start) = self.entries.get(index).ok_or_else(missing_entry)?;
        if !entry.is_file {
            return Err(unsupported("TAR directory entries have no file reader"));
        }
        let mut metadata = self.metadata.clone();
        metadata.content_length = Some(entry.size);
        let input = self.input.clone();
        let end = start + entry.size;
        let stream = try_unfold((input, *start), move |(input, position)| async move {
            if position == end {
                return Ok(None);
            }
            let next = position.saturating_add(chunk_size.get() as u64).min(end);
            let bytes = input.read_range(position..next).await?;
            Ok(Some((bytes, (input, next))))
        });
        Ok(Box::new(StreamReader::new(stream, metadata)))
    }
}
