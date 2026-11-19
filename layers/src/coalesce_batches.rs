use std::num::NonZeroUsize;

use arrow_select::concat::concat_batches;
use futures::stream;
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, RecordBatch, RecordCapability, RecordReader, Result,
    SchemaRef, async_trait, map_record_stream,
};

/// Combine small batches over one opened scan, preserving schema and row order.
///
/// Place outside `ParallelReadLayer` to combine rows from different partitions.
/// Outputs contain at most `max_rows` rows. Estimated input array bytes bound
/// accumulation; a single slice exceeding that budget is emitted alone. Slices
/// can retain shared input buffers, and concatenation allocates output arrays,
/// so this is not a hard bound on process memory. Empty batches are discarded.
///
/// Reads start on the first `next()`. A partial batch waits for more input until
/// a budget, EOF or an error is reached; there is no latency timer. Buffered rows
/// are emitted before an upstream error, which is returned on the following read.
/// Dropping the output releases the source and buffered batches. No task is spawned.
#[derive(Clone, Copy, Debug)]
pub struct CoalesceBatchesLayer {
    max_rows: NonZeroUsize,
    max_estimated_bytes: NonZeroUsize,
}

impl CoalesceBatchesLayer {
    pub fn new(max_rows: NonZeroUsize, max_estimated_bytes: NonZeroUsize) -> Self {
        Self {
            max_rows,
            max_estimated_bytes,
        }
    }
}

/// Accessor decorated by [`CoalesceBatchesLayer`], with independent per-open state.
/// Does not implement `ScanPlanner`: coalescing covers the merged record stream.
#[derive(Clone, Debug)]
pub struct CoalesceBatchesAccessor<A> {
    inner: A,
    settings: CoalesceBatchesLayer,
}

impl<A: Accessor<Reader = dyn RecordReader>> Layer<A> for CoalesceBatchesLayer {
    type Layered = CoalesceBatchesAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        CoalesceBatchesAccessor {
            inner,
            settings: *self,
        }
    }
}

#[async_trait]
impl<A: Accessor<Reader = dyn RecordReader>> Accessor for CoalesceBatchesAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn metadata(
        &self,
        request: Self::Request,
    ) -> Result<open_arrow_access_core::RecordMetadata> {
        self.inner.metadata(request).await
    }

    async fn inspect_metadata(
        &self,
        request: Self::Request,
    ) -> Result<Option<open_arrow_access_core::RecordMetadata>> {
        self.inner.inspect_metadata(request).await
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let inner = self.inner.open(request).await?;
        let settings = self.settings;
        Ok(Box::new(map_record_stream(inner, move |inner| {
            let state = Coalescer {
                schema: inner.metadata().schema.clone(),
                inner: Some(inner),
                pending: None,
                error: None,
                settings,
            };
            stream::try_unfold(state, |mut state| async move {
                let batch = state.next().await?;
                Ok(batch.map(|batch| (batch, state)))
            })
        })))
    }
}

struct Coalescer {
    schema: SchemaRef,
    inner: Option<Box<dyn RecordReader>>,
    pending: Option<RecordBatch>,
    error: Option<Error>,
    settings: CoalesceBatchesLayer,
}

impl Coalescer {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        let mut batches = Vec::new();
        let mut rows = 0;
        let mut bytes = 0usize;
        while rows < self.settings.max_rows.get() {
            let batch = match self.pending.take() {
                Some(batch) => batch,
                None => {
                    let Some(inner) = self.inner.as_mut() else {
                        break;
                    };
                    match inner.next().await {
                        Ok(Some(batch)) => batch,
                        Ok(None) => {
                            self.inner = None;
                            break;
                        }
                        Err(error) => {
                            self.inner = None;
                            self.error = Some(error);
                            break;
                        }
                    }
                }
            };
            if batch.schema() != self.schema {
                return Err(Error::new(
                    ErrorKind::Decode,
                    "coalesce_batches",
                    "batch schema differs from reader schema",
                ));
            }
            if batch.num_rows() == 0 {
                continue;
            }
            let take = batch.num_rows().min(self.settings.max_rows.get() - rows);
            let part = batch.slice(0, take);
            let estimated_bytes = part.get_array_memory_size();
            let remaining_bytes = self
                .settings
                .max_estimated_bytes
                .get()
                .saturating_sub(bytes);
            if !batches.is_empty() && estimated_bytes > remaining_bytes {
                self.pending = Some(batch);
                break;
            }
            if take < batch.num_rows() {
                self.pending = Some(batch.slice(take, batch.num_rows() - take));
            }
            rows += take;
            bytes = bytes.saturating_add(estimated_bytes);
            batches.push(part);
            if bytes >= self.settings.max_estimated_bytes.get() {
                break;
            }
        }
        match batches.len() {
            0 => match self.error.take() {
                Some(error) => Err(error),
                None => Ok(None),
            },
            1 => Ok(batches.pop()),
            _ => concat_batches(&self.schema, &batches)
                .map(Some)
                .map_err(crate::error::coalesce_error),
        }
    }
}
