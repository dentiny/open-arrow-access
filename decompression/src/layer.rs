use std::num::NonZeroUsize;

use open_arrow_access_core::{ByteAccessor, Layer};

use crate::{Codec, DecompressionAccessor};

/// Default maximum decoded bytes emitted by one `next` call.
pub const DEFAULT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

/// A sequential byte transformation. Apply explicitly to compressed inputs;
/// this layer does not inspect file extensions or infer compression.
#[derive(Clone, Debug)]
pub struct DecompressionLayer<C> {
    codec: C,
    chunk_size: NonZeroUsize,
}

impl<C: Codec> DecompressionLayer<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Limit decoded output per chunk, independently of compressed storage I/O.
    /// This does not bound codec state, an upstream chunk or consumer-held output.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

impl<A: ByteAccessor, C: Codec> Layer<A> for DecompressionLayer<C> {
    type Layered = DecompressionAccessor<A, C>;

    fn layer(&self, inner: A) -> Self::Layered {
        DecompressionAccessor::new(inner, self.codec.clone(), self.chunk_size)
    }
}
