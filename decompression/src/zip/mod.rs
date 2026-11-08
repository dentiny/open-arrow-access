//! ZIP directory inspection and selected-entry byte decompression.

mod archive;
mod error;
pub use archive::ZipArchive;

use std::num::NonZeroUsize;

use crate::{archive::DEFAULT_MAX_ENTRY_BYTES, layer::DEFAULT_CHUNK_SIZE};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, FactoryAccessor, FactoryToken,
    ReaderFactory, Result, async_trait,
};

/// Resource limits checked by the ZIP parser, including actual decoded length.
#[derive(Clone, Debug)]
pub struct ZipLimits {
    /// Maximum compressed and decoded size of the selected entry.
    pub max_entry_bytes: u64,
}

impl Default for ZipLimits {
    fn default() -> Self {
        Self {
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
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
    binding: FactoryAccessor<B, ZipFactory>,
}

impl<B> ZipAccessor<B> {
    pub fn new(inner: B, entry_index: usize) -> Self {
        Self {
            binding: FactoryAccessor::new(
                inner,
                ZipFactory {
                    entry_index,
                    limits: ZipLimits::default(),
                    chunk_size: DEFAULT_CHUNK_SIZE,
                },
            ),
        }
    }

    pub fn with_limits(mut self, limits: ZipLimits) -> Self {
        self.binding.factory_mut().limits = limits;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.binding.factory_mut().chunk_size = chunk_size;
        self
    }
}

impl<B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> ZipAccessor<B> {
    /// Read the central directory without decompressing entry bodies.
    /// Storage options are forwarded unchanged. Order is central-directory order.
    pub async fn entries(&self, request: ByteReadRequest) -> Result<Vec<ZipEntry>> {
        Ok(self.archive(request).await?.entries().to_vec())
    }

    /// Inspect once and reuse the same directory/source for multiple entries.
    pub async fn archive(&self, request: ByteReadRequest) -> Result<ZipArchive> {
        let input = self.binding.storage().open(request).await?;
        ZipArchive::open(input, self.binding.factory().limits.clone()).await
    }
}

#[async_trait]
impl<B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor for ZipAccessor<B> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        self.binding.capability()
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.binding.open(request).await
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
        ZipArchive::open(input, self.limits.clone())
            .await?
            .open_entry(self.entry_index, self.chunk_size)
            .await
    }
}
