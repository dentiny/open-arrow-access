//! Byte-budgeted in-memory read-ahead for ordered streaming files.

use std::sync::Arc;

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

// Bound queue bookkeeping even for zero-column or empty batches.
const MIN_BATCH_CHARGE: usize = 1024;

#[derive(Clone)]
pub(crate) struct ReadAheadBudget {
    permits: Arc<Semaphore>,
    limit: usize,
}

impl ReadAheadBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    async fn reserve(
        &self,
        batch: &RecordBatch,
        head: &mut watch::Receiver<bool>,
    ) -> Option<OwnedSemaphorePermit> {
        let bytes = batch.get_array_memory_size().max(MIN_BATCH_CHARGE);
        if bytes > self.limit || u32::try_from(bytes).is_err() {
            // An oversized batch cannot enter read-ahead. Wait until this file
            // becomes the head, then deliver through its ordinary bounded queue.
            let _ = head.wait_for(|head| *head).await;
            return None;
        }
        tokio::select! {
            biased;
            // Promotion must bypass byte admission: a later file may own the
            // entire budget, and its output cannot drain before this file.
            _ = head.wait_for(|head| *head) => None,
            permit = self.permits.clone().acquire_many_owned(bytes as u32) => permit.ok(),
        }
    }
}

pub(crate) struct BufferedBatch {
    batch: RecordBatch,
    _permit: OwnedSemaphorePermit,
}

pub(crate) enum PartitionOutput {
    Batch(RecordBatch),
    ReadAhead(mpsc::Receiver<BufferedBatch>),
}

pub(crate) struct PartitionReceiver {
    pub(crate) batches: BoxStream<'static, Result<RecordBatch>>,
    head: watch::Sender<bool>,
}

impl PartitionReceiver {
    pub(crate) fn new(
        receiver: mpsc::Receiver<PartitionOutput>,
        head: watch::Sender<bool>,
    ) -> Self {
        let batches = stream::try_unfold(
            (receiver, None::<mpsc::Receiver<BufferedBatch>>),
            |(mut receiver, mut buffered)| async move {
                loop {
                    if let Some(input) = &mut buffered {
                        if let Some(BufferedBatch { batch, _permit }) = input.recv().await {
                            // Delivered batches belong to the caller, rather than
                            // the executor's queued-output memory budget.
                            drop(_permit);
                            return Ok(Some((batch, (receiver, buffered))));
                        }
                        buffered = None;
                    }
                    match receiver.recv().await {
                        Some(PartitionOutput::Batch(batch)) => {
                            return Ok(Some((batch, (receiver, buffered))));
                        }
                        Some(PartitionOutput::ReadAhead(input)) => buffered = Some(input),
                        None => return Ok(None),
                    }
                }
            },
        )
        .boxed();
        Self { batches, head }
    }

    pub(crate) fn promote(&self) {
        self.head.send_replace(true);
    }
}

/// Read a non-head file beyond its full Q-batch queue while shared memory permits.
/// False means EOF/cancellation; true resumes ordinary bounded delivery on head
/// promotion. The queue token keeps earlier, buffered and later batches ordered.
pub(crate) async fn buffer_file(
    reader: &mut dyn RecordReader,
    sender: &mpsc::Sender<PartitionOutput>,
    head: &mut watch::Receiver<bool>,
    schema: &SchemaRef,
    budget: &ReadAheadBudget,
) -> Result<bool> {
    let (buffered, input) = mpsc::channel((budget.limit / MIN_BATCH_CHARGE).max(1));
    let mut pending = Some(input);
    loop {
        if sender.is_closed() {
            return Ok(false);
        }
        if pending.is_some()
            && let Ok(permit) = sender.try_reserve()
        {
            permit.send(PartitionOutput::ReadAhead(pending.take().unwrap()));
        }
        let is_head = *head.borrow();
        if is_head {
            drop(buffered);
            if let Some(input) = pending
                && sender
                    .send(PartitionOutput::ReadAhead(input))
                    .await
                    .is_err()
            {
                return Ok(false);
            }
            return Ok(true);
        }
        let Some(batch) = reader.next().await? else {
            drop(buffered);
            if let Some(input) = pending {
                let _ = sender.send(PartitionOutput::ReadAhead(input)).await;
            }
            return Ok(false);
        };
        crate::validation::validate_partition_schema(&batch.schema(), schema)?;
        let permit = tokio::select! {
            _ = sender.closed() => return Ok(false),
            permit = budget.reserve(&batch, head) => permit,
        };
        if let Some(permit) = permit {
            if buffered
                .send(BufferedBatch {
                    batch,
                    _permit: permit,
                })
                .await
                .is_err()
            {
                return Ok(false);
            }
        } else {
            drop(buffered);
            if let Some(input) = pending
                && sender
                    .send(PartitionOutput::ReadAhead(input))
                    .await
                    .is_err()
            {
                return Ok(false);
            }
            return Ok(sender.send(PartitionOutput::Batch(batch)).await.is_ok());
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
