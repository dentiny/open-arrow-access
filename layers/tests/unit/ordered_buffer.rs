use std::sync::Arc;

use arrow_array::Int64Array;
use futures::{StreamExt, stream};
use open_arrow_access_core::{OrderRequirement, RecordMetadata, StreamReader};

use super::*;

const READ_AHEAD_BYTES: usize =
    open_arrow_access_core::ParallelReadOptions::DEFAULT_READ_AHEAD_BYTES.get();

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![value])) as _)]).unwrap()
}

#[tokio::test]
async fn queued_batches_are_released_on_delivery_or_cancellation() {
    for budget in [
        QueueBudget::Batches(1),
        QueueBudget::Bytes(READ_AHEAD_BYTES),
    ] {
        for consume in [false, true] {
            let expected = batch(1);
            let column = Arc::downgrade(expected.column(0));
            let PartitionChannel {
                sender,
                mut batches,
            } = partition_channel(budget);
            assert!(sender.send(expected, None).await);
            assert!(column.upgrade().is_some());
            if consume {
                let delivered = batches.next().await.unwrap();
                assert_eq!(delivered, batch(1));
                assert!(column.upgrade().is_some());
                drop(delivered);
                assert!(column.upgrade().is_none());
            }
            drop(batches);
            assert!(column.upgrade().is_none());
        }
    }
}

#[tokio::test]
async fn file_read_ahead_finishes_without_polling_output_and_keeps_batch_order() {
    let schema = batch(0).schema();
    let metadata = RecordMetadata {
        schema: schema.clone(),
        source: None,
    };
    let mut reader = StreamReader::records(
        stream::iter((0..3).map(|value| Ok(batch(value)))),
        metadata,
        OrderRequirement::PreserveGlobal,
    );
    let PartitionChannel {
        sender,
        mut batches,
    } = partition_channel(QueueBudget::Bytes(READ_AHEAD_BYTES));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sender.read(&mut reader, &schema),
    )
    .await
    .expect("file download must not wait for output consumption")
    .unwrap();
    for value in 0..3 {
        assert_eq!(batches.next().await.unwrap(), batch(value));
    }
    assert!(batches.next().await.is_none());
}

#[tokio::test]
async fn streaming_byte_budget_waits_for_consumption_and_releases_on_drop() {
    use arrow_array::BinaryArray;
    for consume in [false, true] {
        let large = RecordBatch::try_from_iter([(
            "body",
            Arc::new(BinaryArray::from(vec![
                vec![0; READ_AHEAD_BYTES + 1].as_slice(),
            ])) as _,
        )])
        .unwrap();
        let PartitionChannel {
            sender,
            mut batches,
        } = partition_channel(QueueBudget::Bytes(READ_AHEAD_BYTES));
        assert!(sender.send(large.clone(), None).await);
        let pending = tokio::spawn(async move { sender.send(large, None).await });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished(), "a second oversized batch must wait");
        if consume {
            let delivered = batches.next().await.unwrap();
            assert!(pending.await.unwrap());
            assert!(batches.next().await.is_some());
            drop(delivered);
        } else {
            drop(batches);
            assert!(
                !pending.await.unwrap(),
                "cancellation must release the waiting producer"
            );
        }
    }
}

#[tokio::test]
async fn configured_byte_budget_controls_queue_capacity() {
    for capacity in [1, 2, 4] {
        let item = batch(1);
        let bytes = item.get_array_memory_size();
        let PartitionChannel {
            sender,
            mut batches,
        } = partition_channel(QueueBudget::Bytes(bytes * capacity));
        for _ in 0..capacity {
            assert!(sender.send(item.clone(), None).await);
        }
        let pending = sender.send(item, None);
        tokio::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        let _delivered = batches.next().await.unwrap();
        assert!(
            pending.await,
            "delivery must release bytes even while consumer owns the batch"
        );
    }
}
