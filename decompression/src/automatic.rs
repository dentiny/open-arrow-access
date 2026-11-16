//! Per-object suffix routing. Container selection remains explicit.
use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, FactoryAccessor, Layer,
    ListRequest, ObjectStream, Result, async_trait,
};

use crate::CompressionFactory;

/// Compression of one complete object. This is independent of its decoded file format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    Uncompressed,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "bz2")]
    Bzip2,
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "zstd")]
    Zstd,
}

impl Compression {
    /// Recognize registered built-in suffixes, case-insensitively, without I/O.
    /// Disabled codecs return Unsupported before storage opens.
    pub fn from_path(path: &str) -> Result<Self> {
        CompressionFactory::default().builtin_from_path(path)
    }
}

/// Route each opened object's final suffix to an enabled streaming decoder.
/// Unknown suffixes pass through unchanged. This does not sniff magic bytes,
/// select archive entries, rewrite locators, or infer the decoded record format.
#[derive(Clone, Debug)]
pub struct AutoDecompressionLayer {
    factory: CompressionFactory,
}

impl Default for AutoDecompressionLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoDecompressionLayer {
    pub fn new() -> Self {
        Self {
            factory: CompressionFactory::default(),
        }
    }

    /// Use registered codecs and suffixes, including application-defined codecs.
    pub fn with_factory(mut self, factory: CompressionFactory) -> Self {
        self.factory = factory;
        self
    }

    /// Bound each decoded output chunk; pass-through chunks are unchanged.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.factory = self.factory.with_chunk_size(chunk_size);
        self
    }
}

impl<A: ByteAccessor> Layer<A> for AutoDecompressionLayer {
    type Layered = AutoDecompressionAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        AutoDecompressionAccessor {
            binding: FactoryAccessor::new(inner, self.factory.clone()),
        }
    }
}

/// A byte accessor that may decompress each object independently.
#[derive(Clone, Debug)]
pub struct AutoDecompressionAccessor<A> {
    binding: FactoryAccessor<A, CompressionFactory>,
}

#[async_trait]
impl<A: ByteAccessor> Accessor for AutoDecompressionAccessor<A> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let mut output = self.binding.capability();
        output.list = self.binding.storage().capability().list;
        output
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.binding.open(request).await
    }
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for AutoDecompressionAccessor<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.binding.storage().list(request).await
    }
}
