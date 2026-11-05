//! Shared OpenDAL byte access. New backend constructors reuse this implementation.

use std::{num::NonZeroUsize, ops::Range};

use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error, ErrorKind,
    Reader, Result, async_trait,
};
use opendal::Operator;

const DEFAULT_OUTPUT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

#[derive(Clone, Debug)]
pub(crate) struct OpenDalAccessor {
    operator: Operator,
    chunk_size: NonZeroUsize,
}

impl OpenDalAccessor {
    pub(crate) fn new(operator: Operator) -> Self {
        Self {
            operator,
            chunk_size: DEFAULT_OUTPUT_CHUNK_SIZE,
        }
    }

    pub(crate) fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

#[async_trait]
impl Accessor for OpenDalAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let upstream = self.operator.info().capability();
        let read = upstream.read && upstream.stat;
        // This adapter requires stat for length and uses OpenDAL's range-read contract.
        // OpenDAL's listing support is not exposed by this adapter yet.
        Capability {
            read,
            read_with_range: read,
            ..Capability::default()
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let ByteReadRequest { object, options } = request;
        let path = object.locator();
        let metadata = self
            .operator
            .stat(path)
            .await
            .map_err(|e| map_error("stat", path, e))?;
        if !metadata.is_file() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "open",
                "object is not a regular file",
            ));
        }
        let len = metadata.content_length();
        // Preserve projection savings: coalesce adjacent/overlapping ranges,
        // without reading unrequested bytes between separated ranges.
        let mut builder = self
            .operator
            .reader_with(path)
            .content_length_hint(len)
            .concurrent(options.io_concurrency.get())
            .gap(0);
        if let Some(chunk_size) = options.io_chunk_size {
            builder = builder.chunk(chunk_size.get());
        }
        let inner = builder.await.map_err(|e| map_error("open", path, e))?;
        Ok(Box::new(OpenDalReader {
            inner,
            metadata: ByteMetadata {
                source: Some(object),
                content_length: Some(len),
                version: None,
            },
            offset: 0,
            len,
            chunk_size: self.chunk_size.get(),
            finished: false,
        }) as Box<dyn ByteReader>)
    }
}

struct OpenDalReader {
    inner: opendal::Reader,
    metadata: ByteMetadata,
    offset: u64,
    len: u64,
    chunk_size: usize,
    finished: bool,
}

impl OpenDalReader {
    fn validate_range(&self, range: &Range<u64>) -> Result<()> {
        if range.start > range.end || range.end > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "read_range",
                format!(
                    "range {}..{} is outside object length {}",
                    range.start, range.end, self.len,
                ),
            ));
        }
        Ok(())
    }

    async fn read_exact_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.validate_range(&range)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let expected = range.end - range.start;
        let buffer =
            self.inner.read(range).await.map_err(|e| {
                map_error("read", self.metadata.source.as_ref().unwrap().locator(), e)
            })?;
        if buffer.len() as u64 != expected {
            return Err(Error::new(
                ErrorKind::Io,
                "read_range",
                "unexpected short read; input may have changed",
            ));
        }
        Ok(buffer.to_bytes())
    }
}

#[async_trait]
impl Reader for OpenDalReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            ..Capability::default()
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished || self.offset == self.len {
            self.finished = true;
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.chunk_size as u64)
            .min(self.len);
        match self.read_exact_range(self.offset..end).await {
            Ok(bytes) => {
                self.offset = end;
                Ok(Some(bytes))
            }
            Err(error) => {
                self.finished = true;
                Err(error)
            }
        }
    }
}

#[async_trait]
impl ByteReader for OpenDalReader {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.read_exact_range(range).await
    }

    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let lengths = ranges
            .iter()
            .map(|range| {
                self.validate_range(range)?;
                Ok(range.end - range.start)
            })
            .collect::<Result<Vec<_>>>()?;
        let buffers = self.inner.fetch(ranges).await.map_err(|e| {
            map_error(
                "read_ranges",
                self.metadata.source.as_ref().unwrap().locator(),
                e,
            )
        })?;
        buffers
            .into_iter()
            .zip(lengths)
            .map(|(buffer, expected)| {
                if buffer.len() as u64 != expected {
                    return Err(Error::new(
                        ErrorKind::Io,
                        "read_ranges",
                        "unexpected short read; input may have changed",
                    ));
                }
                Ok(buffer.to_bytes())
            })
            .collect()
    }
}

pub(crate) fn map_error(operation: &'static str, path: &str, error: opendal::Error) -> Error {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => ErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        opendal::ErrorKind::Unsupported => ErrorKind::Unsupported,
        _ => ErrorKind::Io,
    };
    Error::new(kind, operation, format!("object {path:?}")).with_source(error)
}
