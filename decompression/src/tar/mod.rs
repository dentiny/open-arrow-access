//! Bounded inspection and selected-entry reads of uncompressed TAR archives.

mod bridge;
mod error;
mod stream;
pub use stream::{TarEntryReader, TarStream};
mod validation;

use std::{num::NonZeroUsize, sync::Arc};

use futures::stream::try_unfold;
use open_arrow_access_core::{ByteReader, Result, StreamReader};

use crate::archive::{ArchiveSource, DEFAULT_MAX_ENTRY_BYTES};
use bridge::{Lease, METADATA_BUDGET};
use error::{decode, missing_entry};
use std::sync::atomic::Ordering;

const DEFAULT_MAX_ENTRIES: usize = 65_536;

#[derive(Clone, Debug)]
pub struct TarLimits {
    /// Maximum decoded entries, excluding package-consumed extension metadata.
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
/// Uses tar::Archive for GNU/USTAR/PAX metadata and file-name decoding.
/// Exposes regular files and directories; sparse files and links remain unsupported.
/// Requires a Tokio runtime for the synchronous package adapter. Compressed
/// TAR requires a separate ranged representation; a sequential gzip decoder cannot
/// supply this input.
#[derive(Clone)]
pub struct TarArchive {
    source: ArchiveSource,
    entries: Arc<[(TarEntry, u64)]>,
}

impl TarArchive {
    pub async fn open(input: Box<dyn ByteReader>, limits: TarLimits) -> Result<Self> {
        let object = input.metadata().source.clone();
        let context = |error: open_arrow_access_core::Error| error.with_object(object.as_ref());
        let metadata = input.metadata().clone();
        let input = open_arrow_access_core::into_async_seek(
            input,
            NonZeroUsize::new(512).unwrap(),
            "open_tar",
        )
        .map_err(context)?;
        let source = ArchiveSource { input, metadata };
        Self::inspect(source, limits).await.map_err(context)
    }

    async fn inspect(source: ArchiveSource, limits: TarLimits) -> Result<Self> {
        let handle = tokio::runtime::Handle::try_current().map_err(error::runtime)?;
        let lease = Arc::new(Lease(tokio_util::sync::CancellationToken::new()));
        let (input, budget) = bridge::reader(source.input.clone(), lease.0.clone(), handle.clone());
        let archive_len = source.input.content_length();
        let entries = handle
            .spawn_blocking(move || {
                let mut archive = tar::Archive::new(input);
                let mut input = archive.entries_with_seek().map_err(error::header_error)?;
                let mut entries = Vec::new();
                loop {
                    budget.store(METADATA_BUDGET, Ordering::Relaxed);
                    let Some(entry) = input.next() else {
                        break;
                    };
                    let entry = entry.map_err(error::header_error)?;
                    let metadata = validation::entry(&entry, entries.len(), &limits)?;
                    let start = entry.raw_file_position();
                    if start
                        .checked_add(metadata.size)
                        .is_none_or(|end| end > archive_len)
                    {
                        return Err(decode("TAR entry exceeds archive length"));
                    }
                    entries.push((metadata, start));
                }
                Ok(entries)
            })
            .await
            .map_err(error::worker)??;
        drop(lease);
        Ok(Self {
            source,
            entries: entries.into(),
        })
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = &TarEntry> {
        self.entries.iter().map(|(entry, _)| entry)
    }

    /// Whole-object length captured while indexing this fixed TAR input.
    pub fn content_length(&self) -> u64 {
        self.source.input.content_length()
    }

    /// Validated payload range for a regular member. Callers can use independent
    /// source readers to fetch these ranges concurrently without sharing the
    /// seek adapter's lock. Preserve the indexed object identity/version.
    pub fn entry_range(&self, index: usize) -> Result<std::ops::Range<u64>> {
        let (entry, start) = self.entries.get(index).ok_or_else(missing_entry)?;
        validation::validate_regular_entry(entry)?;
        Ok(*start..*start + entry.size)
    }

    /// Create a bounded sequential reader without reopening storage or reparsing
    /// headers. Each reader owns its cursor and can outlive this archive.
    pub fn open_entry(
        &self,
        index: usize,
        chunk_size: NonZeroUsize,
    ) -> Result<Box<dyn ByteReader>> {
        let (entry, start) = self.entries.get(index).ok_or_else(missing_entry)?;
        validation::validate_regular_entry(entry)?;
        let mut metadata = self.source.metadata.clone();
        metadata.content_length = Some(entry.size);
        let input = self.source.input.clone();
        let end = start + entry.size;
        let stream = try_unfold((input, *start), move |(input, position)| async move {
            if position == end {
                return Ok(None);
            }
            let next = position.saturating_add(chunk_size.get() as u64).min(end);
            let bytes = input.read_range(position..next).await?;
            Ok(Some((bytes, (input, next))))
        });
        Ok(Box::new(StreamReader::bytes(stream, metadata)))
    }
}
