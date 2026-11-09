use std::{num::NonZeroUsize, sync::Arc};

use async_zip::base::read1::{
    ZipOptions,
    seek::{ZipArchiveInner, ZipArchiveReader},
};
use open_arrow_access_core::{ByteReader, Error, ErrorKind, Result};

use super::{ZipEntry, ZipLimits, error::zip_error};
use crate::{archive::ArchiveSource, decoded_reader};

/// One opened ZIP source with a shared, parsed central directory.
///
/// Clones retain the same opened source and directory. Entry readers own their
/// cursors and survive dropping this session. Source version guarantees remain
/// those supplied by the underlying byte reader.
#[derive(Clone)]
pub struct ZipArchive {
    directory: Arc<ZipArchiveInner>,
    source: ArchiveSource,
    entries: Arc<[ZipEntry]>,
}

impl ZipArchive {
    /// Inspect the central directory once over a ranged, known-length source.
    pub async fn open(input: Box<dyn ByteReader>, limits: ZipLimits) -> Result<Self> {
        let source = ArchiveSource::new(input, "open_zip")?;
        let options = ZipOptions {
            max_uncompressed_size_per_file: limits.max_entry_bytes,
            max_compressed_size_per_file: limits.max_entry_bytes,
            ..ZipOptions::default()
        };
        let parser = ZipArchiveReader::open_with_options(source.input.clone(), options)
            .await
            .map_err(zip_error)?;
        let entries = parser
            .cdrs()
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                super::validation::validate_offset(entry)?;
                Ok(ZipEntry {
                    index,
                    name: entry.insecure_file_name.as_bytes().to_vec(),
                    compressed_size: entry.compressed_size().map_err(zip_error)?,
                    uncompressed_size: entry.uncompressed_size().map_err(zip_error)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            directory: parser.inner().clone(),
            source,
            entries: entries.into(),
        })
    }

    pub fn entries(&self) -> &[ZipEntry] {
        &self.entries
    }

    /// Open an independent selected-entry reader without reopening storage or
    /// reparsing the directory. The returned future owns its session handle.
    pub fn open_entry(
        &self,
        entry_index: usize,
        chunk_size: NonZeroUsize,
    ) -> impl std::future::Future<Output = Result<Box<dyn ByteReader>>> + Send + 'static + use<>
    {
        let archive = self.clone();
        async move { archive.read_entry(entry_index, chunk_size).await }
    }

    async fn read_entry(
        self,
        entry_index: usize,
        chunk_size: NonZeroUsize,
    ) -> Result<Box<dyn ByteReader>> {
        let entry = self.directory.cdrs().get(entry_index).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open_zip_entry",
                "ZIP entry index is out of bounds",
            )
        })?;
        let mut metadata = self.source.metadata;
        metadata.content_length = Some(entry.uncompressed_size().map_err(zip_error)?);
        // ZIP local file header (offsets relative to lfh_offset):
        // | signature (4 B) | version (2 B) | flags (2 B) | ... |
        // 0                 4               6             8
        let flags_offset = entry
            .lfh_offset()
            .map_err(zip_error)?
            .checked_add(6)
            .filter(|offset| {
                offset
                    .checked_add(2)
                    .is_some_and(|end| end <= self.source.input.content_length())
            })
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Decode,
                    "open_zip_entry",
                    "invalid local header offset",
                )
            })?;
        let flags = self
            .source
            .input
            .read_range(flags_offset..flags_offset + 2)
            .await?;
        // SeekableInput validates the exact range length. Encryption flags are
        // bits 0 and 6 of the little-endian field's first byte.
        if flags[0] & 0x41 != 0 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "open_zip_entry",
                "encrypted ZIP entries are unsupported",
            ));
        }
        let parser = ZipArchiveReader::new_with_inner(self.source.input, self.directory);
        let file = parser.file_oneshot(entry_index).await.map_err(zip_error)?;
        Ok(decoded_reader(
            file,
            metadata,
            chunk_size,
            "decompress_zip",
            "ZIP",
        ))
    }
}
