use std::num::NonZeroUsize;

use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, Error, Layer, RecordBatch, RecordCapability, RecordReader, Result, StreamReader,
    async_trait,
};
use tokio::{sync::mpsc, task::JoinSet};

/// Read ahead in one background task, preserving batches, metadata and order.
///
/// Configure a row limit, an estimated-byte limit, or both. With both configured,
/// the queue must fit both budgets. Byte estimates use
/// [`RecordBatch::get_array_memory_size`], including array overhead; shared buffers
/// can be counted more than once. Empty batches cost at least one unit in each
/// enabled budget so they cannot create an unbounded queue.
///
/// Batches are never split. A batch larger than a budget occupies that entire
/// budget and is queued only by itself. In addition to the bounded queue, the
/// producer can hold one batch awaiting admission (its size is unknown before
/// reading). These budgets exclude decoder allocations and caller-owned batches;
/// they are not a hard cap on process memory. Configure decoder batch sizes too.
///
/// Construction and opening start no task and poll no batches. The first `next`
/// requires a Tokio runtime and starts prefetching. Dropping the output reader
/// aborts the worker, including when it is blocked in an upstream read.
/// Place this outside `ParallelReadLayer` for a single scan-wide queue.
#[derive(Clone, Copy, Debug)]
pub struct PrefetchLayer {
    max_rows: Option<NonZeroUsize>,
    max_estimated_bytes: Option<NonZeroUsize>,
}

impl PrefetchLayer {
    /// Bound queued rows. Oversized batches are admitted alone.
    pub fn rows(max_rows: NonZeroUsize) -> Self {
        Self {
            max_rows: Some(max_rows),
            max_estimated_bytes: None,
        }
    }

    /// Bound queued estimated Arrow array bytes. Oversized batches are admitted alone.
    pub fn estimated_bytes(max_estimated_bytes: NonZeroUsize) -> Self {
        Self {
            max_rows: None,
            max_estimated_bytes: Some(max_estimated_bytes),
        }
    }

    /// Set a row budget in addition to any byte budget.
    pub fn with_max_rows(mut self, max_rows: NonZeroUsize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    /// Set a byte budget in addition to any row budget.
    pub fn with_max_estimated_bytes(mut self, max_estimated_bytes: NonZeroUsize) -> Self {
        self.max_estimated_bytes = Some(max_estimated_bytes);
        self
    }

    fn usage(self, rows: usize, bytes: usize) -> BudgetUsage {
        BudgetUsage {
            rows: self.max_rows.map_or(0, |max| rows.max(1).min(max.get())),
            bytes: self
                .max_estimated_bytes
                .map_or(0, |max| bytes.max(1).min(max.get())),
        }
    }
}

/// A record accessor decorated by [`PrefetchLayer`], with independent per-open budgets.
/// Does not expose `ScanPlanner`: its queue covers the opened stream, not partitions.
#[derive(Clone, Debug)]
pub struct PrefetchAccessor<A> {
    inner: A,
    settings: PrefetchLayer,
}

impl<A: Accessor<Reader = dyn RecordReader>> Layer<A> for PrefetchLayer {
    type Layered = PrefetchAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        PrefetchAccessor {
            inner,
            settings: *self,
        }
    }
}

#[async_trait]
impl<A: Accessor<Reader = dyn RecordReader>> Accessor for PrefetchAccessor<A> {
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

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let inner = self.inner.open(request).await?;
        let metadata = inner.metadata().clone();
        let capability = inner.capability();
        let settings = self.settings;
        let batches = stream::once(async move {
            let state = PrefetchState::start(inner, settings)?;
            Ok::<_, Error>(stream::try_unfold(state, |mut state| async move {
                let batch = state.next().await?;
                Ok::<_, Error>(batch.map(|batch| (batch, state)))
            }))
        })
        .try_flatten();
        Ok(Box::new(
            StreamReader::records(batches, metadata, capability.ordering)
                .with_capability(capability),
        ))
    }
}

#[derive(Clone, Copy, Default)]
struct BudgetUsage {
    rows: usize,
    bytes: usize,
}

/// Only the producer owns accounting. Consumption returns credits through a
/// channel, avoiding shared locks and keeping each reservation cancellation-safe.
struct Budget {
    settings: PrefetchLayer,
    used: BudgetUsage,
    releases: mpsc::UnboundedReceiver<BudgetUsage>,
}

impl Budget {
    fn fits(&self, usage: BudgetUsage) -> bool {
        self.settings
            .max_rows
            .is_none_or(|max| usage.rows <= max.get() - self.used.rows)
            && self
                .settings
                .max_estimated_bytes
                .is_none_or(|max| usage.bytes <= max.get() - self.used.bytes)
    }

    async fn wait_for(&mut self, usage: BudgetUsage) -> bool {
        while !self.fits(usage) {
            let Some(released) = self.releases.recv().await else {
                return false;
            };
            self.used.rows -= released.rows;
            self.used.bytes -= released.bytes;
        }
        true
    }
}

async fn produce(
    mut inner: Box<dyn RecordReader>,
    sender: mpsc::UnboundedSender<(RecordBatch, BudgetUsage)>,
    mut budget: Budget,
) -> Result<()> {
    // Reserve no extra batch when either enabled budget is already full.
    while budget.wait_for(budget.settings.usage(1, 1)).await {
        tokio::task::consume_budget().await;
        let Some(batch) = inner.next().await? else {
            break;
        };
        let bytes = if budget.settings.max_estimated_bytes.is_some() {
            batch.get_array_memory_size()
        } else {
            0
        };
        let usage = budget.settings.usage(batch.num_rows(), bytes);
        if !budget.wait_for(usage).await {
            break;
        }
        budget.used.rows += usage.rows;
        budget.used.bytes += usage.bytes;
        if sender.send((batch, usage)).is_err() {
            break;
        }
    }
    Ok(())
}

struct PrefetchState {
    receiver: mpsc::UnboundedReceiver<(RecordBatch, BudgetUsage)>,
    releases: mpsc::UnboundedSender<BudgetUsage>,
    // Dropping the stream aborts the worker, including during a pending read.
    tasks: JoinSet<Result<()>>,
}

impl PrefetchState {
    fn start(inner: Box<dyn RecordReader>, settings: PrefetchLayer) -> Result<Self> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(crate::error::prefetch_runtime_error)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let (releases, released) = mpsc::unbounded_channel();
        let mut tasks = JoinSet::new();
        tasks.spawn_on(
            produce(
                inner,
                sender,
                Budget {
                    settings,
                    used: BudgetUsage::default(),
                    releases: released,
                },
            ),
            &runtime,
        );
        Ok(Self {
            receiver,
            releases,
            tasks,
        })
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if let Some((batch, usage)) = self.receiver.recv().await {
            let _ = self.releases.send(usage);
            return Ok(Some(batch));
        }
        // Drain queued batches before reporting the worker's error or panic.
        if let Some(joined) = self.tasks.join_next().await {
            joined.map_err(crate::error::prefetch_task_error)??;
        }
        Ok(None)
    }
}
