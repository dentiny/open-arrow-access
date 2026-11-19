use std::sync::Arc;

use open_arrow_access_core::{OrderRequirement, Schema};

use super::*;

const READ_AHEAD_BYTES: usize =
    open_arrow_access_core::ParallelReadOptions::DEFAULT_READ_AHEAD_BYTES.get();

#[tokio::test]
async fn refilling_last_slot_does_not_skip_an_existing_ready_queue() {
    let metadata = RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    };
    let batch = RecordBatch::new_empty(metadata.schema.clone());
    let replacement_metadata = metadata.clone();
    let replacement: ScanPartition = Box::pin(async move {
        Ok(
            Box::new(ScanPlan::new(replacement_metadata, vec![]).into_reader())
                as Box<dyn RecordReader>,
        )
    });
    let mut plan =
        ScanPlan::new(metadata, vec![replacement]).with_ordering(OrderRequirement::Reorderable);
    plan.emission = EmissionPolicy::Unordered;
    let mut reader = ParallelReader::new(
        plan,
        ParallelReadOptions {
            partitions: NonZeroUsize::new(2).unwrap(),
            prefetch: NonZeroUsize::MIN,
            read_ahead_bytes: ParallelReadOptions::DEFAULT_READ_AHEAD_BYTES,
        },
    );
    let PartitionChannel {
        sender,
        batches: ready,
    } = partition_channel(1, false, READ_AHEAD_BYTES);
    assert!(sender.send(batch.clone(), None).await);
    drop(sender);
    let PartitionChannel {
        sender,
        batches: closed,
    } = partition_channel(1, false, READ_AHEAD_BYTES);
    drop(sender);
    reader.partition_receivers.extend([closed, ready]);

    // Discovery replaces the closed queue before polling output again.
    // The existing ready queue must retain the next turn.
    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert_eq!(reader.partition_receivers.len(), 2);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}
