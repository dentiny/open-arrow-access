//! Shared OpenDAL byte access. New backend constructors reuse this implementation.

use std::{num::NonZeroUsize, ops::Range};

use open_arrow_access_core::{
    Accessor, BoxFuture, ByteMetadata, ByteReader, Bytes, Capability, Error, ErrorKind, ObjectRef,
    Reader, Result,
};
use opendal::Operator;

#[derive(Clone, Debug)]
pub(crate) struct OpenDalAccessor {
    operator: Operator,
    chunk_size: NonZeroUsize,
}

impl OpenDalAccessor {
    pub(crate) fn new(operator: Operator) -> Self {
        Self {
            operator,
            chunk_size: NonZeroUsize::new(64 * 1024).unwrap(),
        }
    }

    pub(crate) fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

impl Accessor for OpenDalAccessor {
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    /// Map OpenDAL's effective read/stat support to the operations this adapter exposes.
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

    /// Stat the object and create an OpenDAL byte reader with known length and ranges.
    fn open(&self, object: ObjectRef) -> BoxFuture<'_, Result<Box<dyn ByteReader>>> {
        Box::pin(async move {
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
            let inner = self
                .operator
                .reader_with(path)
                .concurrent(1)
                .await
                .map_err(|e| map_error("open", path, e))?;
            let len = metadata.content_length();
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
        })
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
    async fn read_exact_range(&self, range: Range<u64>) -> Result<Bytes> {
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

impl Reader for OpenDalReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    /// Report sequential and exact-range access on the opened byte representation.
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            ..Capability::default()
        }
    }

    /// Borrow the source and byte length captured when the object was opened.
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    /// Read one bounded chunk and advance the cursor only after a successful read.
    /// EOF and read failures terminate subsequent sequential consumption.
    fn next(&mut self) -> BoxFuture<'_, Result<Option<Bytes>>> {
        Box::pin(async move {
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
        })
    }
}

impl ByteReader for OpenDalReader {
    /// Validate and fetch an exact range without moving the sequential cursor.
    fn read_range(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(self.read_exact_range(range))
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
