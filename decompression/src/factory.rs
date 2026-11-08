use std::num::NonZeroUsize;

use open_arrow_access_core::{
    ByteReadRequest, ByteReader, FactoryToken, ReaderFactory, Result, async_trait, into_async_read,
};

use crate::{Codec, decoded_reader, layer::DEFAULT_CHUNK_SIZE};

/// Configure one codec's sequential byte-reader creation.
#[derive(Clone, Debug)]
pub struct DecompressionFactory<C> {
    codec: C,
    chunk_size: NonZeroUsize,
}

impl<C: Codec> DecompressionFactory<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Limit decoded output per chunk, independently of storage I/O chunking.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

#[async_trait]
impl<C: Codec> ReaderFactory for DecompressionFactory<C> {
    type Request = ByteReadRequest;
    type Prepared = ();
    type Reader = dyn ByteReader;

    fn prepare(&self, _request: &ByteReadRequest) -> Result<()> {
        Ok(())
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        _request: ByteReadRequest,
        (): (),
    ) -> Result<Box<dyn ByteReader>> {
        let mut metadata = input.metadata().clone();
        metadata.content_length = None;
        let input = into_async_read(input, "decompress_input");
        let decoder = self.codec.decoder(Box::pin(input));
        Ok(decoded_reader(
            decoder,
            metadata,
            self.chunk_size,
            "decompress",
            self.codec.name(),
        ))
    }
}
