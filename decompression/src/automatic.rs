//! Per-object suffix routing. Container selection remains explicit.
use std::num::NonZeroUsize;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, FactoryAccessor, FactoryToken,
    Layer, ListRequest, ObjectStream, ReaderFactory, Result, async_trait,
};

use crate::layer::DEFAULT_CHUNK_SIZE;

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
    /// Recognize case-sensitive final suffixes without accessing storage.
    /// Locators are opaque: query/fragment characters are not removed or decoded.
    /// ZIP/TAR are containers and are not selected by this function.
    /// Recognized compression with a disabled codec returns `Unsupported`;
    /// it is never silently treated as uncompressed bytes.
    pub fn from_path(path: &str) -> Result<Self> {
        macro_rules! select {
            ($feature:literal, $variant:ident) => {{
                #[cfg(feature = $feature)]
                {
                    Ok(Self::$variant)
                }
                #[cfg(not(feature = $feature))]
                {
                    Err(crate::error::disabled_codec($feature))
                }
            }};
        }
        if path.ends_with(".gz") || path.ends_with(".tgz") {
            select!("gzip", Gzip)
        } else if path.ends_with(".bz2") {
            select!("bz2", Bzip2)
        } else if path.ends_with(".lz4") {
            select!("lz4", Lz4)
        } else if path.ends_with(".zst") || path.ends_with(".zstd") {
            select!("zstd", Zstd)
        } else {
            Ok(Self::Uncompressed)
        }
    }
}

/// Route each opened object's final suffix to an enabled streaming decoder.
/// Unknown suffixes pass through unchanged. This does not sniff magic bytes,
/// select archive entries, rewrite locators, or infer the decoded record format.
#[derive(Clone, Debug)]
pub struct AutoDecompressionLayer {
    chunk_size: NonZeroUsize,
}

impl Default for AutoDecompressionLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoDecompressionLayer {
    pub fn new() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Bound each decoded output chunk; pass-through chunks are unchanged.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

impl<A: ByteAccessor> Layer<A> for AutoDecompressionLayer {
    type Layered = AutoDecompressionAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        AutoDecompressionAccessor {
            binding: FactoryAccessor::new(
                inner,
                AutoFactory {
                    chunk_size: self.chunk_size,
                },
            ),
        }
    }
}

/// A byte accessor that may decompress each object independently.
#[derive(Clone, Debug)]
pub struct AutoDecompressionAccessor<A> {
    binding: FactoryAccessor<A, AutoFactory>,
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

#[derive(Clone, Debug)]
struct AutoFactory {
    chunk_size: NonZeroUsize,
}

#[async_trait]
impl ReaderFactory for AutoFactory {
    type Request = ByteReadRequest;
    type Prepared = Compression;
    type Reader = dyn ByteReader;

    fn capability(&self, input: Capability) -> Capability {
        // Per-object readers can be more specific; the mixed accessor cannot promise ranges.
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn prepare(&self, request: &ByteReadRequest) -> Result<Compression> {
        Compression::from_path(request.object.locator())
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        _request: ByteReadRequest,
        compression: Compression,
    ) -> Result<Box<dyn ByteReader>> {
        let _chunk_size = self.chunk_size;
        // Keeping the concrete codec factories avoids a second decompression implementation.
        #[allow(unused_macros)]
        macro_rules! decode {
            ($codec:expr) => {{
                use open_arrow_access_core::ReaderFactoryExt;
                crate::DecompressionFactory::new($codec)
                    .with_chunk_size(_chunk_size)
                    .open_input(input, _request)
                    .await
            }};
        }
        match compression {
            Compression::Uncompressed => Ok(input),
            #[cfg(feature = "gzip")]
            Compression::Gzip => decode!(crate::gzip::Gzip),
            #[cfg(feature = "bz2")]
            Compression::Bzip2 => decode!(crate::bz2::Bzip2),
            #[cfg(feature = "lz4")]
            Compression::Lz4 => decode!(crate::lz4::Lz4),
            #[cfg(feature = "zstd")]
            Compression::Zstd => decode!(crate::zstd::Zstd),
        }
    }
}
