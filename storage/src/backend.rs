//! Shared OpenDAL byte access. New backend constructors reuse this implementation.

use std::{num::NonZeroUsize, ops::Range};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ObjectRef, ObjectStream, Reader, Result, async_trait,
};
use opendal::Operator;

const DEFAULT_OUTPUT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
const DEFAULT_IO_CHUNK_SIZE: usize = 2 * 1024 * 1024;
const MAX_FETCH_BATCH_SIZE: u64 = 2 * 1024 * 1024;

/// Read bytes through a caller-configured OpenDAL operator.
///
/// Locators are relative to the operator's namespace. Unlike the filesystem adapter
/// this adapter does not constrain host filesystem components. Locators must be
/// canonical root-relative paths without surrounding whitespace, empty or dot
/// components. It exposes no write operations.
#[derive(Clone, Debug)]
pub struct OpenDalAccessor {
    operator: Operator,
    chunk_size: NonZeroUsize,
}

impl OpenDalAccessor {
    /// Wrap an already configured operator without performing I/O.
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            chunk_size: DEFAULT_OUTPUT_CHUNK_SIZE,
        }
    }

    /// Set the maximum sequential output chunk, independently of I/O splitting.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
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
        Capability {
            read,
            read_with_range: read,
            list: upstream.list && upstream.stat,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let ByteReadRequest { object, options } = request;
        let path = object.locator();
        validate_locator(path, false, "open")?;
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
        let builder = self
            .operator
            .reader_with(path)
            .content_length_hint(len)
            .concurrent(options.io_concurrency.get())
            .chunk(
                options
                    .io_chunk_size
                    .map_or(DEFAULT_IO_CHUNK_SIZE, |v| v.get()),
            )
            .gap(0);
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

#[async_trait]
impl ByteAccessor for OpenDalAccessor {
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        if !self.capability().list {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "list",
                "operator does not support listing",
            ));
        }
        let path = prefix.locator();
        validate_locator(path, true, "list")?;
        let metadata = self
            .operator
            .stat(path)
            .await
            .map_err(|e| map_error("list", path, e))?;
        if !metadata.is_dir() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "list",
                "prefix is not a directory",
            ));
        }
        let path = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        let lister = self
            .operator
            .lister_with(&path)
            .recursive(true)
            .await
            .map_err(|e| map_error("list", &path, e))?;
        Ok(Box::pin(
            lister
                .try_filter(|entry| futures::future::ready(entry.metadata().is_file()))
                .map_ok(|entry| ObjectRef::new(entry.path()))
                .map_err(move |e| map_error("list", &path, e)),
        ))
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
        if expected > MAX_FETCH_BATCH_SIZE {
            // Parquet requires contiguous Bytes. Copy incrementally into the
            // final allocation rather than retaining every fragment as well.
            let capacity = usize::try_from(expected).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "read_range",
                    "range exceeds addressable memory",
                )
            })?;
            let mut output = Vec::with_capacity(capacity);
            let read = self
                .inner
                .read_into(&mut output, range)
                .await
                .map_err(|e| {
                    map_error("read", self.metadata.source.as_ref().unwrap().locator(), e)
                })?;
            if read != capacity {
                return Err(short_read("read_range"));
            }
            return Ok(Bytes::from(output));
        }
        let buffer =
            self.inner.read(range).await.map_err(|e| {
                map_error("read", self.metadata.source.as_ref().unwrap().locator(), e)
            })?;
        if buffer.len() as u64 != expected {
            return Err(short_read("read_range"));
        }
        Ok(buffer.to_bytes())
    }

    async fn fetch_ranges(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        let lengths = ranges
            .iter()
            .map(|range| range.end - range.start)
            .collect::<Vec<_>>();
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
                    return Err(short_read("read_ranges"));
                }
                Ok(buffer.to_bytes())
            })
            .collect()
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
        for range in &ranges {
            self.validate_range(range)?;
        }
        let mut output = Vec::with_capacity(ranges.len());
        let mut start = 0;
        // Keep fragmented staging bounded. Large ranges are assembled directly;
        // small ranges retain fetch's coalescing/concurrency within each batch.
        while start < ranges.len() {
            if ranges[start].end - ranges[start].start > MAX_FETCH_BATCH_SIZE {
                output.push(self.read_exact_range(ranges[start].clone()).await?);
                start += 1;
                continue;
            }
            let mut end = start;
            let mut size = 0;
            while end < ranges.len() {
                let length = ranges[end].end - ranges[end].start;
                if length > MAX_FETCH_BATCH_SIZE - size {
                    break;
                }
                size += length;
                end += 1;
            }
            output.extend(self.fetch_ranges(ranges[start..end].to_vec()).await?);
            start = end;
        }
        Ok(output)
    }
}

fn short_read(operation: &'static str) -> Error {
    Error::new(
        ErrorKind::Io,
        operation,
        "unexpected short read; input may have changed",
    )
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

fn validate_locator(path: &str, prefix: bool, operation: &'static str) -> Result<()> {
    let base = if prefix {
        path.strip_suffix('/').unwrap_or(path)
    } else {
        path
    };
    if path.trim() != path
        || path.starts_with('/')
        || (!prefix && path.is_empty())
        || (!base.is_empty()
            && base
                .split('/')
                .any(|part| part.is_empty() || matches!(part, "." | "..")))
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            operation,
            "expected a canonical root-relative locator",
        ));
    }
    Ok(())
}
