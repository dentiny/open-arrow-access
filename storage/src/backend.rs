//! Shared OpenDAL byte access. New backend constructors reuse this implementation.

use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error, ErrorKind,
    Reader, Result, async_trait,
};
#[cfg(feature = "huggingface")]
use open_arrow_access_core::{ByteAccessor, ObjectRef, ObjectStream};
use opendal::{
    Execute, Executor, OperationContext, Operator,
    raw::{BoxedStaticFuture, Layer, Servicer},
};
use tokio::sync::Semaphore;

const DEFAULT_OUTPUT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
const DEFAULT_IO_CHUNK_SIZE: usize = 2 * 1024 * 1024;
const MAX_FETCH_BATCH_SIZE: u64 = 2 * 1024 * 1024;

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

    #[cfg(feature = "huggingface")]
    pub(crate) async fn validate_directory(&self, prefix: &ObjectRef) -> Result<()> {
        let path = prefix.locator();
        let metadata = self
            .operator
            .stat(path)
            .await
            .map_err(|error| map_error("list", path, error))?;
        if !metadata.is_dir() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "list",
                "prefix is not a directory",
            ));
        }
        Ok(())
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
        // Concrete backend wrappers decide whether to expose discovery.
        Capability {
            read,
            read_with_range: read,
            list: false,
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
        // OpenDAL gives each stream/fetch its own task queue. Share a budget
        // across those queues so retained sequential prefetch cannot multiply
        // the reader's I/O concurrency when a range read is interleaved.
        let mut operator = self.operator.clone();
        if options.io_concurrency.get() > 1 {
            operator = operator.layer(ReaderBudget(options.io_concurrency.get()));
        }
        // Preserve projection savings: coalesce adjacent/overlapping ranges,
        // without reading unrequested bytes between separated ranges.
        let builder = operator
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
            sequential: None,
            pending: opendal::Buffer::new(),
            len,
            chunk_size: self.chunk_size.get(),
            finished: false,
        }) as Box<dyn ByteReader>)
    }
}

#[cfg(feature = "huggingface")]
#[async_trait]
impl ByteAccessor for OpenDalAccessor {
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        let path = prefix.locator();
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

#[derive(Debug)]
struct ReaderBudget(usize);

impl Layer for ReaderBudget {
    fn apply_context(&self, _: Servicer, context: OperationContext) -> OperationContext {
        let executor = Executor::with(ReaderExecutor {
            inner: context.executor().clone().into_inner(),
            permits: Arc::new(Semaphore::new(self.0)),
        });
        context.with_executor(executor)
    }
}

struct ReaderExecutor {
    inner: Arc<dyn Execute>,
    permits: Arc<Semaphore>,
}

impl Execute for ReaderExecutor {
    fn execute(&self, future: BoxedStaticFuture<()>) {
        let permits = self.permits.clone();
        self.inner.execute(Box::pin(async move {
            let _permit = permits
                .acquire()
                .await
                .expect("reader budget is never closed");
            future.await;
        }));
    }

    fn timeout(&self) -> Option<BoxedStaticFuture<()>> {
        self.inner.timeout()
    }
}

struct OpenDalReader {
    inner: opendal::Reader,
    sequential: Option<opendal::BufferStream>,
    pending: opendal::Buffer,
    metadata: ByteMetadata,
    offset: u64,
    len: u64,
    chunk_size: usize,
    finished: bool,
}

impl OpenDalReader {
    async fn read_next_chunk(&mut self) -> Result<Bytes> {
        let path = self.metadata.source.as_ref().unwrap().locator();
        if self.sequential.is_none() {
            self.sequential = Some(
                self.inner
                    .clone()
                    .into_stream(0..self.len)
                    .await
                    .map_err(|error| map_error("read", path, error))?,
            );
        }
        let target = (self.len - self.offset).min(self.chunk_size as u64) as usize;
        let mut output = Vec::new();
        let mut written = 0;
        while written < target {
            while self.pending.is_empty() {
                let Some(buffer) = self
                    .sequential
                    .as_mut()
                    .unwrap()
                    .try_next()
                    .await
                    .map_err(|error| map_error("read", path, error))?
                else {
                    return Err(short_read("read"));
                };
                if buffer.len() as u64 > self.len - self.offset {
                    return Err(Error::new(
                        ErrorKind::Io,
                        "read",
                        "input exceeded its declared length",
                    ));
                }
                self.pending = buffer;
            }
            // Most output chunks fit in one storage buffer. Keep that path
            // zero-copy without allocating a vector of fragments.
            if written == 0 && self.pending.current().len() >= target {
                self.offset += target as u64;
                return Ok(self.pending.split_to(target).next().unwrap());
            }
            let count = (target - written).min(self.pending.len());
            output.extend(self.pending.split_to(count));
            self.offset += count as u64;
            written += count;
        }
        Ok(opendal::Buffer::from(output).to_bytes())
    }

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

    async fn read_exact_range(&mut self, range: Range<u64>) -> Result<Bytes> {
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

    async fn fetch_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
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
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished || self.offset == self.len {
            self.finished = true;
            self.sequential = None;
            self.pending = opendal::Buffer::new();
            return Ok(None);
        }
        match self.read_next_chunk().await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) => {
                self.finished = true;
                self.sequential = None;
                self.pending = opendal::Buffer::new();
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
