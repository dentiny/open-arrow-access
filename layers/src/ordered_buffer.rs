//! In-memory read-ahead for concurrency-limited ordered streaming files.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{RecordBatch, RecordReader, Result, SchemaRef};
use tokio::sync::mpsc;

pub(crate) enum PartitionOutput {
    Batch(RecordBatch),
    ReadAhead(mpsc::UnboundedReceiver<RecordBatch>),
}

pub(crate) struct PartitionReceiver {
    pub(crate) batches: BoxStream<'static, Result<RecordBatch>>,
    is_head: Arc<AtomicBool>,
}

impl PartitionReceiver {
    pub(crate) fn new(receiver: mpsc::Receiver<PartitionOutput>, is_head: Arc<AtomicBool>) -> Self {
        let batches = stream::try_unfold(
            (receiver, None::<mpsc::UnboundedReceiver<RecordBatch>>),
            |(mut receiver, mut buffered)| async move {
                loop {
                    if let Some(input) = &mut buffered {
                        if let Some(batch) = input.recv().await {
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
        Self { batches, is_head }
    }

    pub(crate) fn promote(&self) {
        self.is_head.store(true, Ordering::Release);
    }
}

/// Read a non-head file beyond its full Q-batch queue into memory.
/// The executor bounds resident files; this queue has no byte or batch limit.
/// False means EOF/cancellation; true resumes ordinary bounded delivery on head
/// promotion. The queue token keeps earlier, buffered and later batches ordered.
pub(crate) async fn buffer_file(
    reader: &mut dyn RecordReader,
    sender: &mpsc::Sender<PartitionOutput>,
    is_head: &AtomicBool,
    schema: &SchemaRef,
) -> Result<bool> {
    let (buffered, input) = mpsc::unbounded_channel();
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
        if is_head.load(Ordering::Acquire) {
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
        if buffered.send(batch).is_err() {
            return Ok(false);
        }
        // Ready in-memory sources need a cancellation/fairness point now that
        // read-ahead no longer waits for queue capacity or memory admission.
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
#[path = "../tests/unit/ordered_buffer.rs"]
mod tests;
