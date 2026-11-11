use std::sync::Arc;

use open_arrow_access_core::{OrderRequirement, Schema};

use super::*;

fn closed_queue() -> mpsc::Receiver<RecordBatch> {
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    receiver
}

#[tokio::test]
async fn refilling_last_slot_does_not_skip_an_existing_ready_queue() {
    let metadata = RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    };
    let batch = RecordBatch::new_empty(metadata.schema.clone());
    let mut plan = ScanPlan::new(metadata, vec![]).with_ordering(OrderRequirement::Reorderable);
    plan.emission = EmissionPolicy::Unordered;
    let mut reader = ParallelReader::new(
        plan,
        ParallelReadOptions {
            partitions: NonZeroUsize::new(2).unwrap(),
            prefetch: NonZeroUsize::MIN,
        },
    );
    let (sender, receiver) = mpsc::channel(1);
    sender.send(batch.clone()).await.unwrap();
    reader
        .partition_receivers
        .extend([receiver, closed_queue()]);
    reader.next_receiver = 1;

    // A replacement worker can finish before output is polled again.
    reader.remove_receiver(1);
    reader.partition_receivers.push_back(closed_queue());
    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert_eq!(reader.partition_receivers.len(), 2);

    // Removing the final receiver also leaves a valid empty-window cursor.
    reader.remove_receiver(1);
    reader.remove_receiver(0);
    assert_eq!(reader.next_receiver, 0);
    assert!(reader.next().await.unwrap().is_none());
}
