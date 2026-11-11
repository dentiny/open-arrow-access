use std::{collections::VecDeque, num::NonZeroUsize, task::Poll};

use futures::{
    StreamExt,
    future::poll_fn,
    stream::{self, BoxStream, Fuse, FusedStream},
};

use open_arrow_access_core::{
    Accessor, Capability, EmissionPolicy, Layer, OrderRequirement, ParallelReadOptions, Reader,
    RecordBatch, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner,
    async_trait,
};
use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    error::{partition_task_error, schema_error},
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
}

pub struct ParallelReadAccessor<A> {
    inner: A,
    settings: ParallelReadLayer,
}

impl<A: ScanPlanner> Layer<A> for ParallelReadLayer {
    type Layered = ParallelReadAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        ParallelReadAccessor {
            inner,
            settings: *self,
        }
    }
}

#[async_trait]
impl<A: ScanPlanner> Accessor for ParallelReadAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        let read = self.inner.capability().read;
        Capability {
            read,
            read_parallel: read,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        // TODO: After parallel-read implementation is complete, verify support across
        // format, layer, directory, and service combinations, including partition
        // independence, preserved output order, and effective storage capabilities.
        open_plan(self.plan_scan(request).await?)
    }
}

#[async_trait]
impl<A: ScanPlanner> ScanPlanner for ParallelReadAccessor<A> {
    fn order_requirement(&self) -> OrderRequirement {
        self.inner.order_requirement()
    }

    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        let mut plan = self.inner.plan_scan(request).await?;
        let settings = ParallelReadOptions {
            partitions: self.settings.partitions,
            prefetch: self.settings.prefetch,
        };
        validate_parallel_read_settings(plan.parallel_read, settings)?;
        plan.parallel_read = Some(settings);
        Ok(plan)
    }
}

pub(crate) fn open_plan(plan: ScanPlan) -> Result<Box<dyn RecordReader>> {
    validate_emission_policy(plan.emission, plan.ordering)?;
    match plan.parallel_read {
        Some(settings) => Ok(Box::new(ParallelReader::new(plan, settings))),
        None => Ok(Box::new(plan.into_reader())),
    }
}

struct ParallelReader {
    metadata: RecordMetadata,
    remaining: Fuse<BoxStream<'static, Result<ScanPartition>>>,
    partition_receivers: VecDeque<mpsc::Receiver<RecordBatch>>,
    tasks: JoinSet<Result<()>>,
    settings: ParallelReadOptions,
    emission: EmissionPolicy,
    next_receiver: usize,
}

impl ParallelReader {
    fn new(plan: ScanPlan, settings: ParallelReadOptions) -> Self {
        Self {
            metadata: plan.metadata,
            remaining: plan.partitions.fuse(),
            partition_receivers: VecDeque::new(),
            tasks: JoinSet::new(),
            settings,
            emission: plan.emission,
            next_receiver: 0,
        }
    }

    fn spawn_partition(&mut self, open: ScanPartition) {
        let (sender, receiver) = mpsc::channel(self.settings.prefetch.get());
        let schema = self.metadata.schema.clone();
        self.tasks.spawn(async move {
            let mut reader = open.await?;
            if reader.metadata().schema != schema {
                return Err(schema_error());
            }
            loop {
                // Reserve before decoding so a full queue cannot hide an extra batch.
                let Ok(permit) = sender.reserve().await else {
                    return Ok(());
                };
                let Some(batch) = reader.next().await? else {
                    return Ok(());
                };
                if batch.schema() != schema {
                    return Err(schema_error());
                }
                permit.send(batch);
            }
        });
        self.partition_receivers.push_back(receiver);
    }

    fn remove_receiver(&mut self, index: usize) {
        self.partition_receivers.remove(index);
        // Wrap before discovery can append a replacement at the removed tail index.
        self.next_receiver = index
            .checked_rem(self.partition_receivers.len())
            .unwrap_or(0);
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
            let next_receiver = &mut self.next_receiver;
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
                    let count = receivers.len();
                    if emission == EmissionPolicy::Ordered {
                        return receivers[0].poll_recv(cx).map(|batch| (0, batch));
                    }
                    // Rotate the starting point so continuously ready partitions
                    // cannot starve other readers. Each receiver preserves FIFO order.
                    for offset in 0..count {
                        let index = (*next_receiver + offset) % count;
                        if let Poll::Ready(batch) = receivers[index].poll_recv(cx) {
                            *next_receiver = (index + 1) % count;
                            return Poll::Ready((index, batch));
                        }
                    }
                    Poll::Pending
                }), if has_receivers => {
                    if batch.is_some() { return Ok(batch); }
                    // EOF only frees a resident slot after that receiver is drained.
                    self.remove_receiver(index);
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

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_parallel: true,
            read_with_range: false,
            list: false,
        }
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
