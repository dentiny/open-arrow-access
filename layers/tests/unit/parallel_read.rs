use std::sync::Arc;

use open_arrow_access_core::{OrderRequirement, Schema};

use super::*;

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
        },
    );
    let (sender, ready) = mpsc::channel(1);
    assert!(
        sender
            .try_send(QueuedBatch {
                batch: batch.clone(),
                _bytes: None
            })
            .is_ok()
    );
    drop(sender);
    let (sender, closed) = mpsc::channel(1);
    drop(sender);
    reader.partition_receivers.extend([closed, ready]);

    // Discovery replaces the closed queue before polling output again.
    // The existing ready queue must retain the next turn.
    assert_eq!(reader.next().await.unwrap(), Some(batch));
    assert_eq!(reader.partition_receivers.len(), 2);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn byte_budget_waits_until_delivery_or_drop_releases_bytes() {
    use futures::FutureExt;
    let budget = Arc::new(ByteBudget::new(100));
    let first = budget.acquire(70).await.unwrap();
    assert_eq!(budget.available.load(Ordering::Acquire), 30);
    assert!(budget.acquire(40).now_or_never().is_none());
    drop(first);
    let second = budget.acquire(100).await.unwrap();
    assert_eq!(budget.available.load(Ordering::Acquire), 0);
    drop(second);
    assert_eq!(budget.available.load(Ordering::Acquire), 100);
    assert_eq!(
        budget.acquire(101).await.err().unwrap().kind(),
        open_arrow_access_core::ErrorKind::ResourceExhausted
    );
}
