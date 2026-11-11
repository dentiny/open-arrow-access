use std::sync::Arc;

use open_arrow_access_core::{OrderRequirement, Schema};

use super::*;

fn metadata() -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    }
}

fn unordered_reader(parts: Vec<ScanPartition>, partitions: usize) -> ParallelReader {
    let mut plan = ScanPlan::new(metadata(), parts).with_ordering(OrderRequirement::Reorderable);
    plan.emission = EmissionPolicy::Unordered;
    ParallelReader::new(
        plan,
        ParallelReadOptions {
            partitions: NonZeroUsize::new(partitions).unwrap(),
            prefetch: NonZeroUsize::MIN,
        },
    )
}

fn queue(batches: Vec<RecordBatch>) -> mpsc::Receiver<RecordBatch> {
    let (sender, receiver) = mpsc::channel(batches.len().max(1));
    for batch in batches {
        sender.try_send(batch).unwrap();
    }
    receiver
}

#[tokio::test]
async fn refilling_last_slot_does_not_skip_an_existing_ready_queue() {
    // Discovery replaces the closed queue with an empty partition before polling
    // output again. The existing ready queue must retain the next turn.
    let replacement: ScanPartition = Box::pin(async {
        Ok(Box::new(ScanPlan::new(metadata(), vec![]).into_reader()) as Box<dyn RecordReader>)
    });
    let mut reader = unordered_reader(vec![replacement], 2);
    let batch = RecordBatch::new_empty(reader.metadata.schema.clone());
    reader
        .partition_receivers
        .extend([queue(vec![]), queue(vec![batch.clone()])]);

    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert_eq!(reader.partition_receivers.len(), 2);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}
