//! Partition output queues with batch-count or estimated-byte backpressure.

use std::sync::Arc;

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Queued batches retain permits until delivery or cancellation. Native
/// partitions reserve a slot before decoding; streaming files reserve bytes
/// after decoding. Each partition owns its budget to avoid ordered-output deadlocks.
pub(crate) struct PartitionSender {
    sender: mpsc::UnboundedSender<(RecordBatch, OwnedSemaphorePermit)>,
    budget: Arc<Semaphore>,
    read_ahead_bytes: Option<usize>,
}

pub(crate) struct PartitionChannel {
    pub(crate) sender: PartitionSender,
    pub(crate) batches: BoxStream<'static, RecordBatch>,
}

pub(crate) fn partition_channel(
    prefetch: usize,
    read_ahead: bool,
    read_ahead_bytes: usize,
) -> PartitionChannel {
    let (sender, receiver) = mpsc::unbounded_channel();
    let batches = stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|(batch, _permit)| (batch, receiver))
    });
    PartitionChannel {
        sender: PartitionSender {
            sender,
            budget: Arc::new(Semaphore::new(if read_ahead {
                read_ahead_bytes
            } else {
                prefetch
            })),
            read_ahead_bytes: read_ahead.then_some(read_ahead_bytes),
        },
        batches: batches.boxed(),
    }
}

impl PartitionSender {
    async fn reserve(&self, permits: u32) -> OwnedSemaphorePermit {
        self.budget
            .clone()
            .acquire_many_owned(permits)
            .await
            .expect("partition budget is never closed")
    }

    pub(crate) async fn send(
        &self,
        batch: RecordBatch,
        slot: Option<OwnedSemaphorePermit>,
    ) -> bool {
        let permit = match slot {
            Some(slot) => slot,
            None => {
                // Oversized batches occupy the budget alone. One further decoded
                // batch may wait for admission outside the queue.
                let bytes = self
                    .read_ahead_bytes
                    .map_or(1, |limit| batch.get_array_memory_size().clamp(1, limit));
                self.reserve(bytes as u32).await
            }
        };
        self.sender.send((batch, permit)).is_ok()
    }

    pub(crate) async fn read(
        self,
        reader: &mut dyn RecordReader,
        schema: &SchemaRef,
    ) -> Result<()> {
        loop {
            let slot = if self.read_ahead_bytes.is_none() {
                Some(self.reserve(1).await)
            } else {
                None
            };
            if self.sender.is_closed() {
                return Ok(());
            }
            let Some(batch) = reader.next().await? else {
                return Ok(());
            };
            crate::validation::validate_partition_schema(&batch.schema(), schema)?;
            if !self.send(batch, slot).await {
                return Ok(());
            }
            // Small ready batches still cooperate with other partition tasks.
            if self.read_ahead_bytes.is_some() {
                tokio::task::yield_now().await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
