use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use futures::{
    StreamExt,
    future::poll_fn,
    stream::{self, BoxStream, Fuse, FusedStream},
};

use open_arrow_access_core::{
    Accessor, Capability, EmissionPolicy, Layer, OrderRequirement, ParallelReadOptions, Reader,
    RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan,
    ScanPlanner, async_trait,
};
use tokio::{
    sync::{Notify, mpsc},
    task::JoinSet,
};

use crate::{
    error::partition_task_error,
    scan_policy::{ScanPolicy, ScanPolicyAccessor},
    validation::{validate_emission_policy, validate_parallel_read_settings},
};

/// Execute independent partitions concurrently, preserving logical order by default.
/// Requires an active Tokio runtime.
#[derive(Clone, Copy, Debug)]
pub struct ParallelReadLayer {
    /// Maximum resident partitions, including completed partitions with queued batches.
    partitions: NonZeroUsize,
    /// Maximum queued batches per partition; callers may hold additional delivered batches.
    /// Bounds queue counts, not decoder allocations or total bytes; configure format
    /// batch sizes separately.
    prefetch: NonZeroUsize,
}

impl ParallelReadLayer {
    pub fn new(partitions: NonZeroUsize, prefetch: NonZeroUsize) -> Self {
        Self {
            partitions,
            prefetch,
        }
    }

    /// Limit estimated Arrow array bytes queued in each resident partition.
    ///
    /// Total queued estimates are bounded by `partitions * bytes`; a shared global
    /// budget would allow later ordered partitions to starve the first partition.
    /// Uses `RecordBatch::get_array_memory_size` (shared buffers may count more
    /// than once). Excludes decoder state, object overhead, delivered batches and
    /// one decoded batch per worker awaiting admission. This is not a total scan
    /// memory limit. Batches exceeding the limit return `ResourceExhausted`.
    /// The returned layer is a terminal executor: place it outside planning and
    /// ordering layers. It deliberately does not expose `ScanPlanner`, whose
    /// existing public plan cannot carry this additional budget.
    pub fn with_max_queued_bytes_per_partition(
        self,
        bytes: NonZeroUsize,
    ) -> BudgetedParallelReadLayer {
        BudgetedParallelReadLayer {
            parallel: self,
            bytes,
        }
    }
}

/// Terminal parallel executor with independent per-partition queued-byte limits.
/// Construct with [`ParallelReadLayer::with_max_queued_bytes_per_partition`].
#[derive(Clone, Copy, Debug)]
pub struct BudgetedParallelReadLayer {
    parallel: ParallelReadLayer,
    bytes: NonZeroUsize,
}

/// A budgeted executor. Compose planning layers before applying this layer.
pub struct BudgetedParallelReadAccessor<A> {
    policy: ScanPolicyAccessor<A, ParallelReadLayer>,
    bytes: NonZeroUsize,
}

impl<A: ScanPlanner> Layer<A> for BudgetedParallelReadLayer {
    type Layered = BudgetedParallelReadAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        BudgetedParallelReadAccessor {
            policy: ScanPolicyAccessor::new(inner, self.parallel),
            bytes: self.bytes,
        }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for BudgetedParallelReadAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.policy.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        open_plan_with_budget(self.policy.plan_scan(request).await?, Some(self.bytes))
    }
}

pub struct ParallelReadAccessor<A> {
    policy: ScanPolicyAccessor<A, ParallelReadLayer>,
}

impl<A: ScanPlanner> Layer<A> for ParallelReadLayer {
    type Layered = ParallelReadAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        ParallelReadAccessor {
            policy: ScanPolicyAccessor::new(inner, *self),
        }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for ParallelReadAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.policy.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.policy.open(request).await
    }
}

#[async_trait]
impl<A: ScanPlanner> ScanPlanner for ParallelReadAccessor<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        self.policy.plan_scan(request).await
    }
}

impl ScanPolicy for ParallelReadLayer {
    fn read_parallel(&self, inner: RecordCapability) -> bool {
        inner.read
    }

    fn apply(&self, plan: &mut ScanPlan) -> Result<()> {
        let settings = ParallelReadOptions {
            partitions: self.partitions,
            prefetch: self.prefetch,
        };
        validate_parallel_read_settings(plan.parallel_read, settings)?;
        plan.parallel_read = Some(settings);
        Ok(())
    }
}

pub(crate) fn open_plan(plan: ScanPlan) -> Result<Box<dyn RecordReader>> {
    open_plan_with_budget(plan, None)
}

fn open_plan_with_budget(
    plan: ScanPlan,
    budget: Option<NonZeroUsize>,
) -> Result<Box<dyn RecordReader>> {
    validate_emission_policy(plan.emission, plan.ordering)?;
    match plan.parallel_read {
        Some(settings) => Ok(Box::new(ParallelReader::new_with_budget(
            plan, settings, budget,
        ))),
        None => Ok(Box::new(plan.into_reader())),
    }
}

// Each partition has one producer. A waiting decoded batch is deliberately outside
// this queued-byte estimate, but cannot exceed the configured per-partition limit.
struct ByteBudget {
    limit: usize,
    available: AtomicUsize,
    changed: Notify,
}

impl ByteBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            available: AtomicUsize::new(limit),
            changed: Notify::new(),
        }
    }

    async fn acquire(self: &Arc<Self>, bytes: usize) -> Result<BytePermit> {
        if bytes > self.limit {
            return Err(crate::error::queued_batch_too_large(bytes, self.limit));
        }
        loop {
            let changed = self.changed.notified();
            let available = self.available.load(Ordering::Acquire);
            if available >= bytes
                && self
                    .available
                    .compare_exchange(
                        available,
                        available - bytes,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
            {
                return Ok(BytePermit {
                    budget: self.clone(),
                    bytes,
                });
            }
            // notify_one retains a permit if release happens before this await.
            changed.await;
        }
    }
}

struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget
            .available
            .fetch_add(self.bytes, Ordering::Release);
        self.budget.changed.notify_one();
    }
}

struct QueuedBatch {
    batch: RecordBatch,
    _bytes: Option<BytePermit>,
}

struct ParallelReader {
    ordering: OrderRequirement,
    metadata: RecordMetadata,
    remaining: Fuse<BoxStream<'static, Result<ScanPartition>>>,
    partition_receivers: VecDeque<mpsc::Receiver<QueuedBatch>>,
    tasks: JoinSet<Result<()>>,
    settings: ParallelReadOptions,
    emission: EmissionPolicy,
    max_queued_bytes_per_partition: Option<NonZeroUsize>,
}

impl ParallelReader {
    #[cfg(test)]
    fn new(plan: ScanPlan, settings: ParallelReadOptions) -> Self {
        Self::new_with_budget(plan, settings, None)
    }

    fn new_with_budget(
        plan: ScanPlan,
        settings: ParallelReadOptions,
        budget: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            ordering: plan.ordering,
            metadata: plan.metadata,
            remaining: plan.partitions.fuse(),
            partition_receivers: VecDeque::new(),
            tasks: JoinSet::new(),
            settings,
            emission: plan.emission,
            max_queued_bytes_per_partition: budget,
        }
    }

    fn spawn_partition(&mut self, open: ScanPartition) {
        let (sender, receiver) = mpsc::channel(self.settings.prefetch.get());
        let schema = self.metadata.schema.clone();
        let budget = self
            .max_queued_bytes_per_partition
            .map(|bytes| Arc::new(ByteBudget::new(bytes.get())));
        self.tasks.spawn(async move {
            let mut reader = open.await?;
            crate::validation::validate_partition_schema(&reader.metadata().schema, &schema)?;
            loop {
                // Reserve before decoding so a full queue cannot hide an extra batch.
                let Ok(permit) = sender.reserve().await else {
                    return Ok(());
                };
                let Some(batch) = reader.next().await? else {
                    return Ok(());
                };
                crate::validation::validate_partition_schema(&batch.schema(), &schema)?;
                let bytes = match &budget {
                    Some(budget) => Some(budget.acquire(batch.get_array_memory_size()).await?),
                    None => None,
                };
                permit.send(QueuedBatch {
                    batch,
                    _bytes: bytes,
                });
            }
        });
        self.partition_receivers.push_back(receiver);
    }

    fn stop(&mut self) {
        // Dropping the JoinSet aborts workers and leaves no completions to poll.
        self.tasks = JoinSet::new();
        self.partition_receivers.clear();
        self.remaining = stream::empty().boxed().fuse();
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let has_receivers = !self.partition_receivers.is_empty();
            let can_discover = !self.remaining.is_terminated()
                && self.partition_receivers.len() < self.settings.partitions.get();
            let receivers = &mut self.partition_receivers;
            let emission = self.emission;
            tokio::select! {
                biased;
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        joined.map_err(partition_task_error)??;
                    }
                }
                partition = self.remaining.next(), if can_discover => {
                    if let Some(partition) = partition {
                        self.spawn_partition(partition?);
                    }
                }
                (index, batch) = poll_fn(|cx| {
                    if emission == EmissionPolicy::Ordered {
                        return receivers[0].poll_recv(cx).map(|batch| (0, batch));
                    }
                    for (index, receiver) in receivers.iter_mut().enumerate() {
                        if let Poll::Ready(batch) = receiver.poll_recv(cx) {
                            return Poll::Ready((index, batch));
                        }
                    }
                    Poll::Pending
                }), if has_receivers => {
                    if batch.is_some() {
                        // Give the other resident partitions the next turn.
                        if emission == EmissionPolicy::Unordered {
                            self.partition_receivers.rotate_left(index + 1);
                        }
                        return Ok(batch.map(|queued| queued.batch));
                    }
                    // EOF only frees a resident slot after that receiver is drained.
                    self.partition_receivers.remove(index);
                }
                else => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/parallel_read.rs"]
mod tests;

#[async_trait]
impl Reader for ParallelReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_parallel: true,
            read_with_range: false,
            list: false,
        }
        .with_ordering(self.ordering)
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.stop();
        }
        result
    }
}
