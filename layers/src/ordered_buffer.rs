//! Partition output queues for concurrency-limited streaming-file read-ahead.

use std::sync::Arc;

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

/// Delivers a partition worker's decoded batches using its queue policy.
pub(crate) enum PartitionSender {
    /// For independent native partitions: reserve one of the configured prefetch
    /// slots before decoding, so a full queue pauses further reads.
    Bounded(mpsc::Sender<RecordBatch>),
    /// Each streaming partition has its own byte budget so later partitions
    /// cannot prevent the first ordered partition from making progress.
    ReadAhead(ReadAheadSender),
}

/// A partition worker's output sender and matching batch stream.
pub(crate) struct PartitionChannel {
    /// Producer endpoint using the selected buffering policy.
    pub(crate) sender: PartitionSender,
    /// Receiver stream preserving batch order within this partition.
    pub(crate) batches: BoxStream<'static, RecordBatch>,
}

struct BufferedBatch {
    batch: RecordBatch,
    _permit: OwnedSemaphorePermit,
}

pub(crate) struct ReadAheadSender {
    sender: mpsc::UnboundedSender<BufferedBatch>,
    budget: Arc<Semaphore>,
}

impl ReadAheadSender {
    async fn send(&self, batch: RecordBatch) -> bool {
        // One oversized batch may occupy a partition's whole budget. A producer
        // can additionally hold one decoded batch while waiting for admission.
        let bytes = batch.get_array_memory_size().clamp(1, READ_AHEAD_BYTES) as u32;
        let permit = self
            .budget
            .clone()
            .acquire_many_owned(bytes)
            .await
            .expect("read-ahead byte semaphore is never closed");
        self.sender
            .send(BufferedBatch {
                batch,
                _permit: permit,
            })
            .is_ok()
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

pub(crate) fn partition_channel(prefetch: usize, read_ahead: bool) -> PartitionChannel {
    if read_ahead {
        let (sender, receiver) = mpsc::unbounded_channel::<BufferedBatch>();
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver
                .recv()
                .await
                .map(|buffered| (buffered.batch, receiver))
        });
        PartitionChannel {
            sender: PartitionSender::ReadAhead(ReadAheadSender {
                sender,
                budget: Arc::new(Semaphore::new(READ_AHEAD_BYTES)),
            }),
            batches: batches.boxed(),
        }
    } else {
        let (sender, receiver) = mpsc::channel(prefetch);
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        PartitionChannel {
            sender: PartitionSender::Bounded(sender),
            batches: batches.boxed(),
        }
    }
}

impl PartitionSender {
    pub(crate) async fn read(
        self,
        reader: &mut dyn RecordReader,
        schema: &SchemaRef,
    ) -> Result<()> {
        match self {
            Self::Bounded(sender) => loop {
                // Reserve before decoding to preserve ordinary queue backpressure.
                let Ok(permit) = sender.reserve().await else {
                    return Ok(());
                };
                let Some(batch) = reader.next().await? else {
                    return Ok(());
                };
                crate::validation::validate_partition_schema(&batch.schema(), schema)?;
                permit.send(batch);
            },
            Self::ReadAhead(sender) => loop {
                if sender.is_closed() {
                    return Ok(());
                }
                let Some(batch) = reader.next().await? else {
                    return Ok(());
                };
                crate::validation::validate_partition_schema(&batch.schema(), schema)?;
                if !sender.send(batch).await {
                    return Ok(());
                }
                // Small ready batches still cooperate with other partition tasks.
                tokio::task::yield_now().await;
            },
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
