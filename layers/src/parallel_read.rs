use std::{
    collections::VecDeque,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    error::partition_task_error,
    ordered_spill::{SpillReader, SpillWriter},
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
    ordered_file_spill: Option<NonZeroU64>,
}

impl ParallelReadLayer {
    pub fn new(partitions: NonZeroUsize, prefetch: NonZeroUsize) -> Self {
        Self {
            partitions,
            prefetch,
            ordered_file_spill: NonZeroU64::new(1024 * 1024 * 1024),
        }
    }

    /// Bound temporary IPC bytes per resident streaming file for ordered output.
    /// Defaults to 1 GiB. None disables disk prefetch. At the limit, delivery
    /// returns to the bounded batch queue. Native partitions and unordered scans
    /// do not spill. Temporary files are removed on consumption or cancellation.
    pub fn with_ordered_file_spill(mut self, limit: Option<NonZeroU64>) -> Self {
        self.ordered_file_spill = limit;
        self
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

    async fn metadata(
        &self,
        request: Self::Request,
    ) -> Result<open_arrow_access_core::RecordMetadata> {
        self.policy.metadata(request).await
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
            ordered_file_spill: self.ordered_file_spill,
        };
        validate_parallel_read_settings(plan.parallel_read, settings)?;
        plan.parallel_read = Some(settings);
        Ok(())
    }
}

pub(crate) fn open_plan(plan: ScanPlan) -> Result<Box<dyn RecordReader>> {
    validate_emission_policy(plan.emission, plan.ordering)?;
    match plan.parallel_read {
        Some(settings) => Ok(Box::new(ParallelReader::new(plan, settings))),
        None => Ok(Box::new(plan.into_reader())),
    }
}

enum PartitionOutput {
    Batch(RecordBatch),
    Spill(SpillReader),
}

struct PartitionReceiver {
    batches: BoxStream<'static, Result<RecordBatch>>,
    head: Arc<AtomicBool>,
}

impl PartitionReceiver {
    fn new(receiver: mpsc::Receiver<PartitionOutput>, head: Arc<AtomicBool>) -> Self {
        // try_unfold owns the pending spill read, so discovery/task completions
        // cannot cancel a partially read IPC frame and lose its file position.
        let batches = stream::try_unfold(
            (receiver, None::<SpillReader>),
            |(mut receiver, mut spill)| async move {
                loop {
                    if let Some(input) = &mut spill {
                        if let Some(batch) = input.next().await? {
                            return Ok(Some((batch, (receiver, spill))));
                        }
                        spill = None;
                    }
                    match receiver.recv().await {
                        Some(PartitionOutput::Batch(batch)) => {
                            return Ok(Some((batch, (receiver, spill))));
                        }
                        Some(PartitionOutput::Spill(input)) => spill = Some(input),
                        None => return Ok(None),
                    }
                }
            },
        )
        .boxed();
        Self { batches, head }
    }
}

/// Keep downloading a later file while its ordered output waits for the head.
/// Returns false on EOF/cancellation, true when head promotion or the disk quota
/// requires resuming normal bounded in-memory delivery.
async fn spill_file(
    reader: &mut dyn RecordReader,
    sender: &mpsc::Sender<PartitionOutput>,
    head: &AtomicBool,
    schema: &open_arrow_access_core::SchemaRef,
    limit: NonZeroU64,
) -> Result<bool> {
    let (mut writer, input) = SpillWriter::new(limit).await?;
    let mut pending = Some(input);
    loop {
        if sender.is_closed() {
            return Ok(false);
        }
        if pending.is_some()
            && let Ok(permit) = sender.try_reserve()
        {
            permit.send(PartitionOutput::Spill(pending.take().unwrap()));
        }
        if head.load(Ordering::Acquire) {
            writer.finish();
            if let Some(input) = pending
                && sender.send(PartitionOutput::Spill(input)).await.is_err()
            {
                return Ok(false);
            }
            return Ok(true);
        }
        let Some(batch) = reader.next().await? else {
            writer.finish();
            if let Some(input) = pending {
                let _ = sender.send(PartitionOutput::Spill(input)).await;
            }
            return Ok(false);
        };
        crate::validation::validate_partition_schema(&batch.schema(), schema)?;
        if !writer.append(&batch).await? {
            writer.finish();
            if let Some(input) = pending
                && sender.send(PartitionOutput::Spill(input)).await.is_err()
            {
                return Ok(false);
            }
            return Ok(sender.send(PartitionOutput::Batch(batch)).await.is_ok());
        }
    }
}

struct ParallelReader {
    ordering: OrderRequirement,
    metadata: RecordMetadata,
    remaining: Fuse<BoxStream<'static, Result<ScanPartition>>>,
    partition_receivers: VecDeque<PartitionReceiver>,
    file_streams: bool,
    tasks: JoinSet<Result<()>>,
    settings: ParallelReadOptions,
    emission: EmissionPolicy,
}

impl ParallelReader {
    fn new(plan: ScanPlan, settings: ParallelReadOptions) -> Self {
        Self {
            ordering: plan.ordering,
            file_streams: plan.file_streams,
            metadata: plan.metadata,
            remaining: plan.partitions.fuse(),
            partition_receivers: VecDeque::new(),
            tasks: JoinSet::new(),
            settings,
            emission: plan.emission,
        }
    }

    fn spawn_partition(&mut self, open: ScanPartition) {
        let (sender, receiver) = mpsc::channel(self.settings.prefetch.get());
        let schema = self.metadata.schema.clone();
        let head = Arc::new(AtomicBool::new(self.partition_receivers.is_empty()));
        let worker_head = head.clone();
        let spill_limit = (self.file_streams && self.emission == EmissionPolicy::Ordered)
            .then_some(self.settings.ordered_file_spill)
            .flatten();
        self.tasks.spawn(async move {
            let mut reader = open.await?;
            crate::validation::validate_partition_schema(&reader.metadata().schema, &schema)?;
            let mut can_spill = spill_limit;
            loop {
                if !worker_head.load(Ordering::Acquire)
                    && let Some(limit) = can_spill
                {
                    match sender.try_reserve() {
                        Ok(permit) => {
                            let Some(batch) = reader.next().await? else {
                                return Ok(());
                            };
                            crate::validation::validate_partition_schema(&batch.schema(), &schema)?;
                            permit.send(PartitionOutput::Batch(batch));
                            continue;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                    }
                    // Only non-head file workers bypass a full memory queue.
                    // One capped spool is allowed for this resident file.
                    if !spill_file(reader.as_mut(), &sender, &worker_head, &schema, limit).await? {
                        return Ok(());
                    }
                    can_spill = None;
                }
                // The head file and memory-only paths reserve before decoding.
                let Ok(permit) = sender.reserve().await else {
                    return Ok(());
                };
                let Some(batch) = reader.next().await? else {
                    return Ok(());
                };
                crate::validation::validate_partition_schema(&batch.schema(), &schema)?;
                permit.send(PartitionOutput::Batch(batch));
            }
        });
        self.partition_receivers
            .push_back(PartitionReceiver::new(receiver, head));
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
                        return receivers[0].batches.poll_next_unpin(cx).map(|batch| (0, batch));
                    }
                    for (index, receiver) in receivers.iter_mut().enumerate() {
                        if let Poll::Ready(batch) = receiver.batches.poll_next_unpin(cx) {
                            return Poll::Ready((index, batch));
                        }
                    }
                    Poll::Pending
                }), if has_receivers => {
                    let batch = batch.transpose()?;
                    if batch.is_some() {
                        // Give the other resident partitions the next turn.
                        if emission == EmissionPolicy::Unordered {
                            self.partition_receivers.rotate_left(index + 1);
                        }
                        return Ok(batch);
                    }
                    // EOF only frees a resident slot after that receiver is drained.
                    self.partition_receivers.remove(index);
                    if emission == EmissionPolicy::Ordered
                        && let Some(front) = self.partition_receivers.front()
                    {
                        front.head.store(true, Ordering::Release);
                    }
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
