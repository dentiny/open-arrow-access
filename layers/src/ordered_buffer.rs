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

pub(crate) fn partition_channel(
    prefetch: usize,
    read_ahead: bool,
) -> (PartitionSender, BoxStream<'static, RecordBatch>) {
    if read_ahead {
        let (sender, receiver) = mpsc::unbounded_channel();
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        (PartitionSender::ReadAhead(sender), batches.boxed())
    } else {
        let (sender, receiver) = mpsc::channel(prefetch);
        let batches = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        (PartitionSender::Bounded(sender), batches.boxed())
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
