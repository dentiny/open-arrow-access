use std::num::NonZeroUsize;

use futures::io::AsyncReadExt;
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Bytes, Capability, Error, ErrorKind, ReadState, Reader, Result,
    async_trait, into_async_read,
};

use crate::{Codec, DecodedInput};

pub(crate) struct DecompressionReader {
    decoder: DecodedInput,
    metadata: ByteMetadata,
    codec: &'static str,
    chunk_size: usize,
    state: ReadState,
}

impl DecompressionReader {
    pub(crate) fn new<C: Codec>(
        inner: Box<dyn ByteReader>,
        codec: &C,
        chunk_size: NonZeroUsize,
    ) -> Self {
        let mut metadata = inner.metadata().clone();
        metadata.content_length = None;
        let input = into_async_read(inner, "decompress_input");
        Self {
            decoder: codec.decoder(Box::pin(input)),
            metadata,
            codec: codec.name(),
            chunk_size: chunk_size.get(),
            state: ReadState::default(),
        }
    }
}

#[async_trait]
impl Reader for DecompressionReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.state.is_finished() {
            return Ok(None);
        }
        let mut output = vec![0; self.chunk_size];
        let result = match self.decoder.read(&mut output).await {
            Ok(0) => Ok(None),
            Ok(len) => {
                output.truncate(len);
                Ok(Some(Bytes::from(output)))
            }
            Err(error) => Err(Error::new(
                ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
                "decompress",
                format!("{} decoding failed", self.codec),
            )
            .with_source(error)),
        };
        self.state.observe(&result);
        result
    }
}

#[async_trait]
impl ByteReader for DecompressionReader {}
