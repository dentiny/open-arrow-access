use std::{collections::VecDeque, num::NonZeroUsize, vec::IntoIter};

use open_arrow_access_core::{
    Accessor, Capability, Layer, Reader, RecordBatch, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};
use tokio::{sync::mpsc, task::JoinSet};

use crate::error::{partition_task_error, schema_error};

/// Execute independent partitions concurrently, emitting them in logical order.
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
        let plan = self.inner.plan_scan(request).await?;
        Ok(Box::new(ParallelReader::new(plan, self.settings)))
    }
}

struct ParallelReader {
    metadata: RecordMetadata,
    remaining: IntoIter<ScanPartition>,
    partition_receivers: VecDeque<mpsc::Receiver<RecordBatch>>,
    tasks: JoinSet<Result<()>>,
    settings: ParallelReadLayer,
    // Set after EOF or an error so later next() calls return EOF without restarting work.
    terminal: bool,
}

impl ParallelReader {
    fn new(plan: ScanPlan, settings: ParallelReadLayer) -> Self {
        Self {
            metadata: plan.metadata,
            remaining: plan.partitions.into_iter(),
            partition_receivers: VecDeque::new(),
            tasks: JoinSet::new(),
            settings,
            terminal: false,
        }
    }

    fn fill(&mut self) {
        while self.partition_receivers.len() < self.settings.partitions.get() {
            let Some(open) = self.remaining.next() else {
                break;
            };
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
    }

    fn stop(&mut self) {
        self.terminal = true;
        self.tasks.abort_all();
        self.partition_receivers.clear();
        self.remaining = Vec::new().into_iter();
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            self.fill();
            let Some(front) = self.partition_receivers.front_mut() else {
                // Channel closure alone is not proof that its worker succeeded.
                while let Some(joined) = self.tasks.join_next().await {
                    joined.map_err(partition_task_error)??;
                }
                return Ok(None);
            };
            tokio::select! {
                biased;
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Some(joined) = joined {
                        joined.map_err(partition_task_error)??;
                    }
                }
                batch = front.recv() => {
                    if batch.is_some() { return Ok(batch); }
                    // A completed worker keeps its slot until all queued batches drain.
                    self.partition_receivers.pop_front();
                }
            }
        }
    }
}

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
        if self.terminal {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.stop();
        }
        result
    }
}
