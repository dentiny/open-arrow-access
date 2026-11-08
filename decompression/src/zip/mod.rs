//! ZIP directory inspection and selected-entry byte decompression.

mod input;

use std::num::NonZeroUsize;

use async_zip::{
    base::read1::{ZipOptions, seek::ZipArchiveReader},
    error::ZipError,
    spec::extra::{EFD, EFHID},
};
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind,
    FactoryToken, ReaderFactory, ReaderFactoryExt, Result, StreamReader, async_trait, read_chunks,
};

use self::input::{SharedInput, ZipInput};
use crate::layer::DEFAULT_CHUNK_SIZE;

/// Resource limits checked by the ZIP parser, including actual decoded length.
#[derive(Clone, Debug)]
pub struct ZipLimits {
    /// Maximum number of entries declared by the central directory.
    pub max_entries: u64,
    /// Maximum declared size of the central directory.
    pub max_directory_bytes: u64,
    /// Maximum compressed and decoded size of the selected entry.
    pub max_entry_bytes: u64,
}

impl Default for ZipLimits {
    fn default() -> Self {
        Self {
            max_entries: 65_536,
            max_directory_bytes: 16 * 1024 * 1024,
            max_entry_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// One central-directory entry. Names are original bytes, never filesystem paths.
/// Duplicate names retain distinct indices; directory entries are retained too.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZipEntry {
    pub index: usize,
    pub name: Vec<u8>,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

/// Decompress one ZIP entry selected by central-directory index.
///
/// Requests identify the archive in the underlying storage. This accessor can
/// feed existing sequential format readers. Use `entries` to inspect names and
/// indices first. Stored and Deflate entries, ZIP64 and data descriptors are
/// supported; encrypted and other compression methods are unsupported.
/// Input requires exact ranges and a known length. Output is sequential only.
#[derive(Clone, Debug)]
pub struct ZipAccessor<B> {
    inner: B,
    factory: ZipFactory,
}

impl<B> ZipAccessor<B> {
    pub fn new(inner: B, entry_index: usize) -> Self {
        Self {
            inner,
            factory: ZipFactory {
                entry_index,
                limits: ZipLimits::default(),
                chunk_size: DEFAULT_CHUNK_SIZE,
            },
        }
    }

    pub fn with_limits(mut self, limits: ZipLimits) -> Self {
        self.factory.limits = limits;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.factory.chunk_size = chunk_size;
        self
    }
}

impl<B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> ZipAccessor<B> {
    /// Read the bounded central directory without decompressing entry bodies.
    /// Storage options are forwarded unchanged. Order is central-directory order.
    pub async fn entries(&self, request: ByteReadRequest) -> Result<Vec<ZipEntry>> {
        let input = self.inner.open(request).await?;
        let archive = open_archive(input, &self.factory.limits).await?;
        archive
            .parser
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
            .collect()
    }
}

#[async_trait]
impl<B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor for ZipAccessor<B> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.factory.capability(self.inner.capability())
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.factory.open_reader(&self.inner, request).await
    }
}

// ZIP entry inspection is separate from underlying storage-prefix listing.
#[async_trait]
impl<B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> ByteAccessor
    for ZipAccessor<B>
{
}

#[derive(Clone, Debug)]
struct ZipFactory {
    entry_index: usize,
    limits: ZipLimits,
    chunk_size: NonZeroUsize,
}

#[async_trait]
impl ReaderFactory for ZipFactory {
    type Request = ByteReadRequest;
    type Prepared = ();
    type Reader = dyn ByteReader;

    fn capability(&self, input: Capability) -> Capability {
        Capability {
            read: input.read && input.read_with_range,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn prepare(&self, _: &ByteReadRequest) -> Result<()> {
        Ok(())
    }

    async fn create_reader(
        &self,
        _: &FactoryToken,
        input: Box<dyn ByteReader>,
        _: ByteReadRequest,
        (): (),
    ) -> Result<Box<dyn ByteReader>> {
        let mut metadata = input.metadata().clone();
        let archive = open_archive(input, &self.limits).await?;
        let entry = archive.parser.cdrs().get(self.entry_index).ok_or_else(|| {
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
            .filter(|offset| offset.checked_add(2).is_some_and(|end| end <= archive.len))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Decode,
                    "open_zip_entry",
                    "invalid local header offset",
                )
            })?;
        let flags = archive
            .source
            .lock()
            .await
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
        let file = archive
            .parser
            .file_oneshot(self.entry_index)
            .await
            .map_err(zip_error)?;
        let stream = read_chunks(file, self.chunk_size).map_err(|error| {
            Error::new(
                ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
                "decompress_zip",
                "ZIP entry decoding failed",
            )
            .with_source(error)
        });
        Ok(Box::new(StreamReader::new(stream, metadata)))
    }
}

struct OpenedArchive {
    parser: ZipArchiveReader<ZipInput>,
    source: SharedInput,
    len: u64,
}

async fn open_archive(input: Box<dyn ByteReader>, limits: &ZipLimits) -> Result<OpenedArchive> {
    if !input.capability().read_with_range {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "open_zip",
            "ZIP requires range reads",
        ));
    }
    let len = input.metadata().content_length.ok_or_else(|| {
        Error::new(
            ErrorKind::Unsupported,
            "open_zip",
            "ZIP requires a known input length",
        )
    })?;
    let options = ZipOptions {
        max_cd_num_files: limits.max_entries,
        max_cd_num_files_load: limits.max_entries,
        max_cd_size_in_bytes: limits.max_directory_bytes,
        max_uncompressed_size_per_file: limits.max_entry_bytes,
        max_compressed_size_per_file: limits.max_entry_bytes,
        ..ZipOptions::default()
    };
    let input = ZipInput::new(input, len);
    let source = input.reader.clone();
    let parser = ZipArchiveReader::open_with_options(input, options)
        .await
        .map_err(zip_error)?;
    Ok(OpenedArchive {
        parser,
        source,
        len,
    })
}

fn zip_error(error: ZipError) -> Error {
    let kind = ErrorKind::from_source(&error).unwrap_or(match error {
        ZipError::FeatureNotSupported(_)
        | ZipError::CompressionNotSupported(_)
        | ZipError::TargetZip64NotSupported => ErrorKind::Unsupported,
        _ => ErrorKind::Decode,
    });
    Error::new(kind, "read_zip", "ZIP archive operation failed").with_source(error)
}
