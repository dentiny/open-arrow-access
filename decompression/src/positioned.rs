use crate::{Codec, CompressedInput, DecodedFrame};
use futures::io::{AsyncBufReadExt, AsyncReadExt};
use open_arrow_access_core::{
    ByteMetadata, BytePosition, ByteReader, Bytes, Capability, Error, ErrorKind, PositionObserver,
    Reader, Result, async_trait, into_positioned_async_read,
};
use std::num::NonZeroUsize;

type Chunk = Option<(Option<BytePosition>, Bytes)>;
struct FrameReader<C> {
    codec: C,
    metadata: ByteMetadata,
    input: Option<CompressedInput>,
    observer: PositionObserver,
    decoder: Option<DecodedFrame>,
    frame: Option<u64>,
    decoded: u64,
    chunk_size: usize,
    pending: Result<Chunk>,
    ended: bool,
    started: bool,
}
pub(crate) async fn open<C: Codec>(
    input: Box<dyn ByteReader>,
    metadata: ByteMetadata,
    codec: C,
    chunk_size: NonZeroUsize,
) -> Result<Box<dyn ByteReader>> {
    let input = into_positioned_async_read(input, "decompress_input");
    let observer = input.observer();
    let mut reader = FrameReader {
        codec,
        metadata,
        input: Some(Box::pin(input)),
        observer,
        decoder: None,
        frame: None,
        decoded: 0,
        chunk_size: chunk_size.get(),
        pending: Ok(None),
        ended: false,
        started: false,
    };
    // Resolve the next frame before exposing position() to consumers.
    reader.pending = Ok(reader.pull().await?);
    Ok(Box::new(reader))
}
impl<C: Codec> FrameReader<C> {
    async fn pull(&mut self) -> Result<Chunk> {
        let result = self.pull_inner().await;
        if result.is_err() {
            self.decoder = None;
            self.input = None;
        }
        result.map_err(|error| error.with_object(self.metadata.source.as_ref()))
    }
    async fn pull_inner(&mut self) -> Result<Chunk> {
        loop {
            if self.decoder.is_none() {
                let mut input = self.input.take().unwrap();
                if input
                    .fill_buf()
                    .await
                    .map_err(|e| self.decode_error(e))?
                    .is_empty()
                {
                    if !self.started {
                        return Err(Error::new(
                            ErrorKind::Decode,
                            "decompress",
                            "empty compressed input",
                        ));
                    }
                    return Ok(None);
                }
                self.started = true;
                self.frame = match self.observer.position() {
                    Some(BytePosition::Stored(n)) => Some(n),
                    _ => None,
                };
                self.decoded = 0;
                self.decoder = Some(self.codec.frame_decoder(input)?);
            }
            let mut output = vec![0; self.chunk_size];
            let n = self
                .decoder
                .as_mut()
                .unwrap()
                .read(&mut output)
                .await
                .map_err(|e| self.decode_error(e))?;
            if n == 0 {
                self.input = Some(self.decoder.take().unwrap().into_input());
                continue;
            }
            output.truncate(n);
            let position = self.frame.map(|frame| BytePosition::Frame {
                frame,
                decoded: self.decoded,
            });
            self.decoded += n as u64;
            return Ok(Some((position, Bytes::from(output))));
        }
    }
    fn decode_error(&self, error: std::io::Error) -> Error {
        Error::new(
            ErrorKind::from_source(&error).unwrap_or(ErrorKind::Decode),
            "decompress",
            format!("{} decoding failed", self.codec.name()),
        )
        .with_source(error)
    }
}
#[async_trait]
impl<C: Codec> Reader for FrameReader<C> {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    type Capability = Capability;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            ..Default::default()
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.ended {
            return Ok(None);
        }
        match std::mem::replace(&mut self.pending, Ok(None)) {
            Ok(Some((_, bytes))) => {
                self.pending = self.pull().await;
                Ok(Some(bytes))
            }
            Ok(None) => {
                self.ended = true;
                Ok(None)
            }
            Err(error) => {
                self.ended = true;
                Err(error)
            }
        }
    }
}
#[async_trait]
impl<C: Codec> ByteReader for FrameReader<C> {
    fn position(&self) -> Option<BytePosition> {
        match &self.pending {
            Ok(Some((position, _))) => *position,
            Ok(None) => self.frame.map(|frame| BytePosition::Frame {
                frame,
                decoded: self.decoded,
            }),
            Err(_) => None,
        }
    }
}
