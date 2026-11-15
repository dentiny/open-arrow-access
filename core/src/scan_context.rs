use std::sync::{Arc, Mutex};

use futures::{
    Future, FutureExt,
    channel::oneshot,
    future::{BoxFuture, Shared},
};

use crate::{
    Accessor, Error, ErrorKind, Reader, RecordBatch, RecordCapability, RecordMetadata,
    RecordReader, Result, async_trait,
};

#[derive(Clone, Copy)]
enum Stop {
    Cancelled,
    Deadline,
}

struct Cancellation {
    sender: Mutex<Option<oneshot::Sender<()>>>,
}

/// Optional, runtime-independent cancellation for one scan and its preparation.
///
/// Clones share explicit cancellation. `run` races opening/planning futures against
/// cancellation; `open` also wraps the returned record reader. `wrap_reader` covers
/// readers obtained through another preparation API. Cancelling a pending read
/// drops that future and its reader, rather than attempting to resume a decoder.
/// Dropping a parallel reader aborts its workers; task destruction is cooperative
/// with its runtime and may complete after the cancellation error is returned.
/// No detached task is spawned by this context. CPU-bound work that does not yield
/// cannot be preempted. A context does not impose a memory budget.
#[derive(Clone)]
pub struct ScanContext {
    cancellation: Arc<Cancellation>,
    stopped: Shared<BoxFuture<'static, Stop>>,
}

impl Default for ScanContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ScanContext {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            cancellation: Arc::new(Cancellation {
                sender: Mutex::new(Some(sender)),
            }),
            stopped: async move {
                let _ = receiver.await;
                Stop::Cancelled
            }
            .boxed()
            .shared(),
        }
    }

    /// Cancel this context and its clones. Safe to call repeatedly from any thread.
    pub fn cancel(&self) {
        if let Some(sender) = self
            .cancellation
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = sender.send(());
        }
    }

    /// Also stop when the caller's deadline future completes, returning `Timeout`.
    ///
    /// For example, pass `tokio::time::sleep(duration)` inside a Tokio runtime.
    /// The context creates no timer or runtime. The deadline is polled during
    /// operations; attach it before cloning to share it across the whole scan.
    pub fn with_deadline(self, deadline: impl Future<Output = ()> + Send + 'static) -> Self {
        let stopped = self.stopped;
        Self {
            cancellation: self.cancellation,
            stopped: async move {
                futures::pin_mut!(stopped, deadline);
                futures::select_biased! {
                    stop = stopped.fuse() => stop,
                    () = deadline.fuse() => Stop::Deadline,
                }
            }
            .boxed()
            .shared(),
        }
    }

    /// Run a fallible operation, dropping it if cancellation/deadline wins.
    /// Already-cancelled operations are never polled. Useful for bind or plan calls.
    pub async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        let stopped = self.stopped.clone().fuse();
        let operation = operation.fuse();
        futures::pin_mut!(stopped, operation);
        futures::select_biased! {
            stop = stopped => Err(match stop {
                Stop::Cancelled => Error::new(ErrorKind::Cancelled, "scan_context", "scan cancelled"),
                Stop::Deadline => Error::new(ErrorKind::Timeout, "scan_context", "scan deadline elapsed"),
            }),
            result = operation => result,
        }
    }

    /// Cancel both accessor opening and subsequent record consumption.
    pub async fn open<A>(&self, accessor: &A, request: A::Request) -> Result<Box<dyn RecordReader>>
    where
        A: Accessor<Reader = dyn RecordReader> + ?Sized,
    {
        let reader = self.run(accessor.open(request)).await?;
        Ok(self.wrap_reader(reader))
    }

    /// Add cancellation to an owned record reader without changing its schema.
    pub fn wrap_reader(&self, reader: Box<dyn RecordReader>) -> Box<dyn RecordReader> {
        Box::new(ContextReader {
            metadata: reader.metadata().clone(),
            capability: reader.capability(),
            reader: Some(reader),
            context: self.clone(),
        })
    }
}

struct ContextReader {
    metadata: RecordMetadata,
    capability: RecordCapability,
    reader: Option<Box<dyn RecordReader>>,
    context: ScanContext,
}

#[async_trait]
impl Reader for ContextReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    fn capability(&self) -> RecordCapability {
        self.capability
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        let result = self.context.run(reader.next()).await;
        if !matches!(result, Ok(Some(_))) {
            self.reader.take();
        }
        result
    }
}
