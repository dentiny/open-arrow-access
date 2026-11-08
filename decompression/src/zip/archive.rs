use std::{num::NonZeroUsize, sync::Arc};

use async_zip::base::read1::{
    ZipOptions,
    seek::{ZipArchiveInner, ZipArchiveReader},
};
use async_zip::spec::extra::{EFD, EFHID};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Error, ErrorKind, Result, SeekableInput, into_async_seek,
};

use super::{ZipEntry, ZipLimits, zip_error};
use crate::{decoded_reader, layer::DEFAULT_CHUNK_SIZE};

/// One opened ZIP source with a shared, parsed central directory.
///
/// Clones retain the same opened source and directory. Entry readers own their
/// cursors and survive dropping this session. Source version guarantees remain
/// those supplied by the underlying byte reader.
#[derive(Clone)]
pub struct ZipArchive {
    directory: Arc<ZipArchiveInner>,
    input: SeekableInput,
    metadata: ByteMetadata,
    entries: Arc<[ZipEntry]>,
}

impl ZipArchive {
    /// Inspect the central directory once over a ranged, known-length source.
    pub async fn open(input: Box<dyn ByteReader>, limits: ZipLimits) -> Result<Self> {
        let metadata = input.metadata().clone();
        let input = into_async_seek(input, DEFAULT_CHUNK_SIZE, "open_zip")?;
        let options = ZipOptions {
            max_cd_num_files: limits.max_entries,
            max_cd_num_files_load: limits.max_entries,
            max_cd_size_in_bytes: limits.max_directory_bytes,
            max_uncompressed_size_per_file: limits.max_entry_bytes,
            max_compressed_size_per_file: limits.max_entry_bytes,
            ..ZipOptions::default()
        };
        let source = input.clone();
        let parser = ZipArchiveReader::open_with_options(input, options)
            .await
            .map_err(zip_error)?;
        let entries = parser
            .cdrs()
            .iter()
            .enumerate()
            .map(|(index, entry)| {
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
            input: source,
            metadata,
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
        let mut metadata = self.metadata;
        let entry = self.directory.cdrs().get(entry_index).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                "open_zip_entry",
                "ZIP entry index is out of bounds",
            )
        })?;
        metadata.content_length = Some(entry.uncompressed_size().map_err(zip_error)?);
        // async_zip's ZIP64 offset accessor assumes the extra field includes
        // an offset. Reject incomplete fields before that accessor is called.
        if entry.cdrh.lfh_offset == u32::MAX {
            match entry.find_ef(EFHID::EI64).map(|field| &field.efd) {
                Some(EFD::EI64(data)) if data.relative_offset.is_some() => (),
                _ => {
                    return Err(Error::new(
                        ErrorKind::Decode,
                        "open_zip_entry",
                        "missing ZIP64 local header offset",
                    ));
                }
            }
        }
        // The upstream parser exposes GPF as an opaque type. Check the local
        // flags before opening a decoder, including empty encrypted entries.
        let flags_offset = entry
            .lfh_offset()
            .map_err(zip_error)?
            .checked_add(6)
            .filter(|offset| {
                offset
                    .checked_add(2)
                    .is_some_and(|end| end <= self.input.content_length())
            })
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Decode,
                    "open_zip_entry",
                    "invalid local header offset",
                )
            })?;
        let flags = self
            .input
            .read_range(flags_offset..flags_offset + 2)
            .await?;
        let flags: [u8; 2] = flags.as_ref().try_into().map_err(|_| {
            Error::new(
                ErrorKind::Io,
                "open_zip_entry",
                "range reader returned an incorrect length",
            )
        })?;
        if u16::from_le_bytes(flags) & 0x41 != 0 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "open_zip_entry",
                "encrypted ZIP entries are unsupported",
            ));
        }
        let parser = ZipArchiveReader::new_with_inner(self.input, self.directory);
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
