use std::{collections::VecDeque, num::NonZeroUsize, vec::IntoIter};

use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, Layer, Reader, RecordBatch, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait,
};
use tokio::{sync::mpsc, task::JoinSet};

/// Execute independent partitions concurrently, emitting them in logical order.
/// Requires an active Tokio runtime. Each partition has at most `prefetch` queued
/// batches plus one delivered batch held by the caller. This bounds queue counts,
/// not decoder allocations or total bytes; configure format batch sizes separately.
#[derive(Clone, Copy, Debug)]
pub struct ParallelReadLayer {
    partitions: NonZeroUsize,
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
        Capability {
            read: self.inner.capability().read,
            read_parallel: self.inner.capability().read,
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
    queues: VecDeque<mpsc::Receiver<RecordBatch>>,
    tasks: JoinSet<Result<()>>,
    settings: ParallelReadLayer,
    terminal: bool,
}

impl ParallelReader {
    fn new(plan: ScanPlan, settings: ParallelReadLayer) -> Self {
        Self {
            metadata: plan.metadata,
            remaining: plan.partitions.into_iter(),
            queues: VecDeque::new(),
            tasks: JoinSet::new(),
            settings,
            terminal: false,
        }
    }

    fn fill(&mut self) {
        while self.queues.len() < self.settings.partitions.get() {
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
            self.queues.push_back(receiver);
        }
    }

    fn stop(&mut self) {
        self.terminal = true;
        self.tasks.abort_all();
        self.queues.clear();
        self.remaining = Vec::new().into_iter();
    }

    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            self.fill();
            let Some(front) = self.queues.front_mut() else {
                // Channel closure alone is not proof that its worker succeeded.
                while let Some(joined) = self.tasks.join_next().await {
                    match joined {
                        Ok(result) => result?,
                        Err(error) => {
                            return Err(Error::new(
                                ErrorKind::Decode,
                                "parallel_read",
                                format!("partition task failed: {error}"),
                            ));
                        }
                    }
                }
                return Ok(None);
            };
            tokio::select! {
                biased;
                joined = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match joined {
                        Some(Ok(result)) => result?,
                        Some(Err(error)) => return Err(Error::new(ErrorKind::Decode,
                            "parallel_read", format!("partition task failed: {error}"))),
                        None => {},
                    }
                }
                batch = front.recv() => {
                    if batch.is_some() { return Ok(batch); }
                    // A completed worker keeps its slot until all queued batches drain.
                    self.queues.pop_front();
                }
            }
        }
    }
}

fn schema_error() -> Error {
    Error::new(
        ErrorKind::Decode,
        "parallel_read",
        "partition schema differs from scan schema",
    )
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
