use std::sync::Arc;

use arrow_array::Int64Array;
use futures::{StreamExt, stream};
use open_arrow_access_core::{OrderRequirement, RecordMetadata, StreamReader};

use super::*;

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![value])) as _)]).unwrap()
}

#[tokio::test]
async fn queued_batches_are_released_on_delivery_or_cancellation() {
    for read_ahead in [false, true] {
        for consume in [false, true] {
            let expected = batch(1);
            let column = Arc::downgrade(expected.column(0));
            let PartitionChannel {
                sender,
                mut batches,
            } = partition_channel(1, read_ahead);
            match sender {
                PartitionSender::Bounded(sender) => sender.send(expected).await.unwrap(),
                PartitionSender::ReadAhead(sender) => sender.send(expected).unwrap(),
            }
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
    } = partition_channel(1, true);
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
