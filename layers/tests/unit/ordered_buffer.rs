use arrow_array::Int64Array;
use futures::StreamExt;
use open_arrow_access_core::{OrderRequirement, Reader, RecordMetadata, StreamReader};
use tokio::sync::oneshot;

use super::*;

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![value])) as _)]).unwrap()
}

#[tokio::test]
async fn read_ahead_batches_are_released_on_delivery_or_cancellation() {
    for consume in [false, true] {
        let expected = batch(1);
        let column = Arc::downgrade(expected.column(0));
        let is_head = Arc::new(AtomicBool::new(false));
        let (buffered, input) = mpsc::unbounded_channel();
        buffered.send(expected).unwrap();
        drop(buffered);
        let (sender, receiver) = mpsc::channel(1);
        assert!(sender.send(PartitionOutput::ReadAhead(input)).await.is_ok());
        drop(sender);
        let mut receiver = PartitionReceiver::new(receiver, is_head);
        assert!(column.upgrade().is_some());
        if consume {
            let delivered = receiver.batches.next().await.unwrap().unwrap();
            assert_eq!(delivered, batch(1));
            assert!(column.upgrade().is_some());
            drop(delivered);
            assert!(column.upgrade().is_none());
        }
        drop(receiver);
        assert!(column.upgrade().is_none());
    }
}

#[tokio::test]
async fn head_promotion_keeps_buffered_and_subsequent_batches_in_order() {
    let is_head = Arc::new(AtomicBool::new(false));
    let worker_head = is_head.clone();
    let (sender, receiver) = mpsc::channel(1);
    assert!(sender.send(PartitionOutput::Batch(batch(0))).await.is_ok());
    let (started, decoded) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let schema = batch(1).schema();
    let metadata = RecordMetadata {
        schema: schema.clone(),
        source: None,
    };
    let records = stream::once(async move {
        started.send(()).unwrap();
        resume.await.unwrap();
        Ok(batch(1))
    })
    .chain(stream::once(async { Ok(batch(2)) }));
    let mut reader = StreamReader::records(records, metadata, OrderRequirement::PreserveGlobal);
    let task = tokio::spawn(async move {
        assert!(
            buffer_file(&mut reader, &sender, &worker_head, &schema)
                .await
                .unwrap()
        );
        // Follow the production worker's ordinary delivery path after promotion.
        while let Some(batch) = reader.next().await.unwrap() {
            assert!(sender.send(PartitionOutput::Batch(batch)).await.is_ok());
        }
    });
    decoded.await.unwrap();
    let mut receiver = PartitionReceiver::new(receiver, is_head);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        receiver.promote();
        release.send(()).unwrap();
        for value in 0..3 {
            assert_eq!(
                receiver.batches.next().await.unwrap().unwrap(),
                batch(value)
            );
        }
        task.await.unwrap();
        assert!(receiver.batches.next().await.is_none());
    })
    .await
    .expect("promotion must preserve the order across both output queues");
}
