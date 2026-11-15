use std::sync::{Arc, Mutex};

use futures::{
    Future, FutureExt,
    channel::oneshot,
    future::{BoxFuture, Shared},
};

use crate::{
    Reader, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result, async_trait,
};

/// Shared, cooperative cancellation for opening and consuming record sources.
///
/// Use `run` for opening/planning futures and `wrap_reader` for their readers.
/// Cancelling a pending read drops both the read future and its reader; decoders
/// are never resumed after cancellation. Clones share one signal, and all pending
/// operations are woken. This creates no runtime, timer or background task.
/// Work that never yields cannot be preempted. Dropping a parallel reader aborts
/// its tasks, whose destruction completes when their runtime schedules it.
#[derive(Clone)]
pub struct ReadCancellation {
    sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    cancelled: Shared<BoxFuture<'static, ()>>,
}

impl Default for ReadCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadCancellation {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            cancelled: async move {
                let _ = receiver.await;
            }
            .boxed()
            .shared(),
        }
    }

    /// Cancel this token and all clones, from any thread. Repeated calls are harmless.
    pub fn cancel(&self) {
        let sender = self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
        // Release the mutex before waking consumers.
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }

    /// Race an operation against cancellation, dropping it if cancellation wins.
    /// An already-cancelled operation is never polled. Upstream errors pass through.
    pub async fn run<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        let cancelled = self.cancelled.clone().fuse();
        let operation = operation.fuse();
        futures::pin_mut!(cancelled, operation);
        futures::select_biased! {
            () = cancelled => Err(crate::error::read_cancelled()),
            result = operation => result,
        }
    }

    /// Cancel pending `next()` calls and release the owned record reader.
    /// EOF, errors and cancellation are terminal; metadata remains available.
    pub fn wrap_reader(&self, reader: Box<dyn RecordReader>) -> Box<dyn RecordReader> {
        Box::new(CancellableReader {
            metadata: reader.metadata().clone(),
            capability: reader.capability(),
            reader: Some(reader),
            cancellation: self.clone(),
        })
    }
}

struct CancellableReader {
    metadata: RecordMetadata,
    capability: RecordCapability,
    reader: Option<Box<dyn RecordReader>>,
    cancellation: ReadCancellation,
}

#[async_trait]
impl Reader for CancellableReader {
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
        let result = self.cancellation.run(reader.next()).await;
        if !matches!(result, Ok(Some(_))) {
            self.reader.take();
        }
        result
    }
}
