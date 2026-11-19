//! Partition output queues with batch-count or estimated-byte backpressure.

use std::sync::Arc;

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// A partition queue is bounded by either batch count or estimated Arrow bytes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueBudget {
    Batches(usize),
    Bytes(usize),
}

/// Queued batches retain permits until delivery or cancellation. Native
/// partitions reserve a slot before decoding; streaming files reserve bytes
/// after decoding. Each partition owns its budget to avoid ordered-output deadlocks.
pub(crate) struct PartitionSender {
    sender: mpsc::UnboundedSender<(RecordBatch, OwnedSemaphorePermit)>,
    permits: Arc<Semaphore>,
    budget: QueueBudget,
}

pub(crate) struct PartitionChannel {
    pub(crate) sender: PartitionSender,
    pub(crate) batches: BoxStream<'static, RecordBatch>,
}

pub(crate) fn partition_channel(budget: QueueBudget) -> PartitionChannel {
    let capacity = match budget {
        QueueBudget::Batches(count) | QueueBudget::Bytes(count) => count,
    };
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
            permits: Arc::new(Semaphore::new(capacity)),
            budget,
        },
        batches: batches.boxed(),
    }
}

impl PartitionSender {
    async fn reserve(&self, permits: u32) -> OwnedSemaphorePermit {
        self.permits
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
                let permits = match self.budget {
                    QueueBudget::Batches(_) => 1,
                    QueueBudget::Bytes(limit) => batch.get_array_memory_size().clamp(1, limit),
                };
                self.reserve(permits as u32).await
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
            let slot = if matches!(self.budget, QueueBudget::Batches(_)) {
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
            if matches!(self.budget, QueueBudget::Bytes(_)) {
                tokio::task::yield_now().await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
