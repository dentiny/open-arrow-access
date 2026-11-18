//! Partition output queues for concurrency-limited streaming-file read-ahead.

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::mpsc;

/// Delivers a partition worker's decoded batches using its queue policy.
pub(crate) enum PartitionSender {
    /// For independent native partitions: reserve one of the configured prefetch
    /// slots before decoding, so a full queue pauses further reads.
    Bounded(mpsc::Sender<RecordBatch>),
    /// For whole streaming files: queue batches in memory without waiting for
    /// consumption. Concurrency limits resident files; queued bytes are unbounded.
    ReadAhead(mpsc::UnboundedSender<RecordBatch>),
}

/// A partition worker's output sender and matching batch stream.
pub(crate) struct PartitionChannel {
    /// Producer endpoint using the selected buffering policy.
    pub(crate) sender: PartitionSender,
    /// Receiver stream preserving batch order within this partition.
    pub(crate) batches: BoxStream<'static, RecordBatch>,
}

pub(crate) fn partition_channel(prefetch: usize, read_ahead: bool) -> PartitionChannel {
    if read_ahead {
        let (sender, receiver) = mpsc::unbounded_channel();
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        PartitionChannel {
            sender: PartitionSender::ReadAhead(sender),
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
                if sender.send(batch).is_err() {
                    return Ok(());
                }
                // Ready sources remain cooperative without queue-capacity waits.
                tokio::task::yield_now().await;
            },
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
