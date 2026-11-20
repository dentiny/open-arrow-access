//! Shared OpenDAL byte access.

mod reader;

use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use crate::error::map_error;
use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, ListRequest, ObjectRef,
    ObjectStream, Result, async_trait,
};
use opendal::{
    Execute, Executor, OperationContext, Operator,
    raw::{BoxedStaticFuture, Layer, Servicer},
};
use reader::{RangedReader, SequentialReader};
use tokio::sync::Semaphore;

const DEFAULT_OUTPUT_CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();
const DEFAULT_IO_CHUNK_SIZE: usize = 2 * 1024 * 1024;

/// Generic OpenDAL byte adapter, shared by storage and provider services.
#[derive(Clone, Debug)]
pub struct OpenDalAccessor {
    operator: Operator,
    http_concurrency_limit: crate::HttpConcurrencyLimitLayer,
    chunk_size: NonZeroUsize,
    content_lengths: Option<Arc<HashMap<String, u64>>>,
}

impl OpenDalAccessor {
    /// Apply optional retry/timeout policies to the supplied OpenDAL operator.
    pub fn try_new(operator: Operator, config: crate::StorageConfig) -> Result<Self> {
        Ok(Self::new(config.apply(operator)?))
    }

    /// Limit this accessor and its clones to 64 ongoing HTTP requests by default.
    /// Independently constructed accessors have separate budgets.
    pub fn new(operator: Operator) -> Self {
        Self {
            operator,
            http_concurrency_limit: crate::HttpConcurrencyLimitLayer::default(),
            chunk_size: DEFAULT_OUTPUT_CHUNK_SIZE,
            content_lengths: None,
        }
    }

    /// Replace the default HTTP request budget for this accessor's readers and clones.
    /// Pass clones of the same limiter to other accessors or provider connections
    /// to include their requests in that budget. Per-reader I/O settings remain separate.
    pub fn with_http_concurrency_limit(mut self, limit: crate::HttpConcurrencyLimitLayer) -> Self {
        self.http_concurrency_limit = limit;
        self
    }

    fn operator(&self) -> Operator {
        self.operator
            .clone()
            .layer(self.http_concurrency_limit.clone())
    }

    pub fn with_content_lengths(
        mut self,
        lengths: impl IntoIterator<Item = (String, u64)>,
    ) -> Self {
        self.content_lengths = Some(Arc::new(lengths.into_iter().collect()));
        self
    }

    /// Bound emitted chunks independently of storage I/O splitting.
    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    async fn content_length(&self, path: &str) -> Result<u64> {
        if let Some(len) = self
            .content_lengths
            .as_ref()
            .and_then(|lengths| lengths.get(path))
        {
            return Ok(*len);
        }
        let metadata = self
            .operator()
            .stat(path)
            .await
            .map_err(|e| map_error("stat", path, e))?;
        crate::validation::validate_regular_file(metadata.is_file())?;
        Ok(metadata.content_length())
    }

    async fn open_with_length(
        &self,
        request: ByteReadRequest,
        len: u64,
    ) -> Result<Box<dyn ByteReader>> {
        let ByteReadRequest {
            object, options, ..
        } = request;
        let path = object.locator();
        // Share a budget across OpenDAL's stream/fetch queues so interleaved
        // range reads cannot multiply sequential prefetch concurrency.
        let mut operator = self.operator();
        if options.io_concurrency.get() > 1 {
            operator = operator.layer(ReaderBudget(options.io_concurrency.get()));
        }
        // Coalesce adjacent ranges without fetching unrequested gaps.
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
        let stream = inner
            .clone()
            .into_stream(0..len)
            .await
            .map_err(|error| map_error("read", path, error))?;
        let sequential = SequentialReader::new(stream, object, len, self.chunk_size);
        Ok(Box::new(RangedReader::new(inner, sequential)))
    }
}

#[async_trait]
impl Accessor for OpenDalAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let upstream = self.operator.info().capability();
        let has_lengths = self
            .content_lengths
            .as_ref()
            .is_some_and(|lengths| !lengths.is_empty());
        let read = upstream.read && (upstream.stat || has_lengths);
        Capability {
            read,
            read_with_range: read,
            read_parallel: false,
            list: upstream.list,
        }
    }

    async fn open_sequential(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let path = request.object.locator();
        if request.content_length.is_some()
            || request.options.io_chunk_size.is_some()
            || request.options.io_concurrency.get() > 1
            || self
                .content_lengths
                .as_ref()
                .is_some_and(|lengths| lengths.contains_key(path))
        {
            return self.open(request).await;
        }
        let reader = self
            .operator()
            .reader(path)
            .await
            .map_err(|e| map_error("open", path, e))?;
        // An unbounded stream avoids both a HEAD and an oversized exact range
        // for tiny objects. Keep this response for all sequential consumption.
        // Explicit splitting options above retain the known-length chunked path.
        let mut stream = reader
            .into_stream(..)
            .await
            .map_err(|e| map_error("open", path, e))?;
        let len = match stream.metadata().await {
            Ok(metadata) => {
                crate::validation::validate_regular_file(metadata.is_file())?;
                metadata.content_length()
            }
            // Some backends (including Fs) cannot expose read-response metadata.
            Err(error) if error.kind() == opendal::ErrorKind::Unsupported => {
                // Release any response/HTTP permit before opening the fallback.
                // Known lengths also avoid oversized filesystem read buffers.
                drop(stream);
                return self.open(request).await;
            }
            Err(error) => return Err(map_error("open", path, error)),
        };
        Ok(Box::new(SequentialReader::new(
            stream,
            request.object,
            len,
            self.chunk_size,
        )))
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        let len = match request.content_length {
            Some(length) => length,
            None => self.content_length(request.object.locator()).await?,
        };
        self.open_with_length(request, len).await
    }
}

#[async_trait]
impl ByteAccessor for OpenDalAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        crate::validation::validate_listing(self.capability().list)?;
        let path = request.prefix.locator();
        let path = if path.is_empty() {
            String::new()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        let lister = self
            .operator()
            .lister_with(&path)
            .recursive(request.recursive)
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
