use std::{io, num::NonZeroUsize};

use futures::{TryStreamExt, io::AsyncReadExt};
use open_arrow_access_core::{
    ByteMetadata, ByteReader, Bytes, Capability, Error, ErrorKind, Reader, Result, async_trait,
    into_stream,
};

use crate::{Codec, DecodedInput};

pub(crate) struct DecompressionReader {
    decoder: DecodedInput,
    metadata: ByteMetadata,
    codec: &'static str,
    chunk_size: usize,
    finished: bool,
}

impl DecompressionReader {
    pub(crate) fn new<C: Codec>(
        inner: Box<dyn ByteReader>,
        codec: &C,
        chunk_size: NonZeroUsize,
    ) -> Self {
        let mut metadata = inner.metadata().clone();
        metadata.content_length = None;
        let input = into_stream(inner)
            .map_err(io::Error::other)
            .into_async_read();
        Self {
            decoder: codec.decoder(Box::pin(input)),
            metadata,
            codec: codec.name(),
            chunk_size: chunk_size.get(),
            finished: false,
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
            ..Capability::default()
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished {
            return Ok(None);
        }
        let mut output = vec![0; self.chunk_size];
        match self.decoder.read(&mut output).await {
            Ok(0) => {
                self.finished = true;
                Ok(None)
            }
            Ok(len) => {
                output.truncate(len);
                Ok(Some(Bytes::from(output)))
            }
            Err(error) => {
                self.finished = true;
                Err(Error::new(
                    ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
                    "decompress",
                    format!("{} decoding failed", self.codec),
                )
                .with_source(error))
            }
        }
    }
}

#[async_trait]
impl ByteReader for DecompressionReader {}
