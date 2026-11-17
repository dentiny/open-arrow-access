//! Optional frame-aware input owned entirely by the provenance layer.
use async_compression::futures::bufread::{BzDecoder, GzipDecoder, Lz4Decoder, ZstdDecoder};
use futures::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, BytePosition, ByteReadRequest, ByteReader, Bytes,
    Capability, Layer, ListRequest, ObjectStream, PositionObserver, Reader, Result, async_trait,
    into_positioned_async_read,
};
use std::{num::NonZeroUsize, pin::Pin};

type Input = Pin<Box<dyn AsyncBufRead + Send>>;
trait Frame: AsyncRead + Send + Unpin {
    fn into_input(self: Box<Self>) -> Input;
}
macro_rules! frame {
    ($decoder:ident) => {
        impl Frame for $decoder<Input> {
            fn into_input(self: Box<Self>) -> Input {
                (*self).into_inner()
            }
        }
    };
}
frame!(GzipDecoder);
frame!(BzDecoder);
frame!(ZstdDecoder);
frame!(Lz4Decoder);
#[derive(Clone, Copy)]
enum Codec {
    Gzip,
    Bzip2,
    Zstd,
    Lz4,
}
impl Codec {
    fn from_path(path: &str) -> Option<Self> {
        match path.rsplit('.').next()?.to_ascii_lowercase().as_str() {
            "gz" | "tgz" => Some(Self::Gzip),
            "bz2" => Some(Self::Bzip2),
            "zst" | "zstd" => Some(Self::Zstd),
            "lz4" => Some(Self::Lz4),
            _ => None,
        }
    }
    fn decoder(self, input: Input) -> Box<dyn Frame> {
        macro_rules! decoder {
            ($t:ident) => {{
                let mut decoder = $t::new(input);
                decoder.multiple_members(false);
                Box::new(decoder)
            }};
        }
        match self {
            Self::Gzip => decoder!(GzipDecoder),
            Self::Bzip2 => decoder!(BzDecoder),
            Self::Zstd => decoder!(ZstdDecoder),
            Self::Lz4 => decoder!(Lz4Decoder),
        }
    }
}
/// Opt-in byte input for SourceColumnsLayer. Install on raw storage, in place
/// of a decompression layer. Uncompressed bytes pass through unchanged.
#[derive(Clone, Debug)]
pub struct SourcePositionLayer {
    chunk_size: NonZeroUsize,
}
impl Default for SourcePositionLayer {
    fn default() -> Self {
        Self {
            chunk_size: NonZeroUsize::new(64 * 1024).unwrap(),
        }
    }
}
impl SourcePositionLayer {
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}
pub struct SourcePositionAccessor<A> {
    inner: A,
    chunk_size: NonZeroUsize,
}
impl<A: ByteAccessor> Layer<A> for SourcePositionLayer {
    type Layered = SourcePositionAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SourcePositionAccessor {
            inner,
            chunk_size: self.chunk_size,
        }
    }
}
#[async_trait]
impl<A: ByteAccessor> Accessor for SourcePositionAccessor<A> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            read_with_range: false,
            ..self.inner.capability()
        }
    }
    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let codec = Codec::from_path(request.object.locator());
        let input = self.inner.open(request.clone()).await?;
        crate::validation::validate_raw_source_position(input.position(), &request.object)?;
        let Some(codec) = codec else {
            return Ok(input);
        };
        let mut metadata = input.metadata().clone();
        metadata.source = Some(request.object);
        metadata.content_length = None;
        let input = into_positioned_async_read(input, "source_positions");
        let observer = input.observer();
        let mut reader = FrameReader {
            codec,
            metadata,
            input: Some(Box::pin(input)),
            observer,
            decoder: None,
            frame: None,
            decoded: 0,
            chunk_size: self.chunk_size.get(),
            pending: Ok(None),
            ended: false,
            started: false,
        };
        reader.pending = Ok(reader.pull().await?);
        Ok(Box::new(reader))
    }
}
#[async_trait]
impl<A: ByteAccessor> ByteAccessor for SourcePositionAccessor<A> {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        self.inner.list(request).await
    }
}
type Chunk = Option<(Option<BytePosition>, Bytes)>;
struct FrameReader {
    codec: Codec,
    metadata: ByteMetadata,
    input: Option<Input>,
    observer: PositionObserver,
    decoder: Option<Box<dyn Frame>>,
    frame: Option<u64>,
    decoded: u64,
    chunk_size: usize,
    pending: Result<Chunk>,
    ended: bool,
    started: bool,
}
impl FrameReader {
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
                    .map_err(crate::error::source_frame_decode_error)?
                    .is_empty()
                {
                    crate::validation::validate_compressed_source_end(self.started)?;
                    return Ok(None);
                }
                self.started = true;
                self.frame = match self.observer.position() {
                    Some(BytePosition::Stored(n)) => Some(n),
                    _ => None,
                };
                self.decoded = 0;
                self.decoder = Some(self.codec.decoder(input));
            }
            let mut output = vec![0; self.chunk_size];
            let n = self
                .decoder
                .as_mut()
                .unwrap()
                .read(&mut output)
                .await
                .map_err(crate::error::source_frame_decode_error)?;
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
}
#[async_trait]
impl Reader for FrameReader {
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
impl ByteReader for FrameReader {
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
