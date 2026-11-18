use std::sync::Arc;

use arrow_array::Int64Array;
use futures::StreamExt;
use open_arrow_access_core::{OrderRequirement, RecordMetadata, StreamReader};
use tokio::sync::oneshot;

use super::*;

fn batch(value: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![value])) as _)]).unwrap()
}

#[tokio::test]
async fn queued_output_owns_memory_credits_until_delivery_or_cancellation() {
    for consume in [false, true] {
        let budget = ReadAheadBudget::new(MIN_BATCH_CHARGE);
        let expected = batch(1);
        let (head, mut worker_head) = watch::channel(false);
        let permit = budget.reserve(&expected, &mut worker_head).await.unwrap();
        assert_eq!(budget.permits.available_permits(), 0);
        let (buffered, input) = mpsc::channel(1);
        buffered
            .send(BufferedBatch {
                batch: expected.clone(),
                _permit: permit,
            })
            .await
            .ok()
            .unwrap();
        drop(buffered);
        let (sender, receiver) = mpsc::channel(1);
        assert!(sender.send(PartitionOutput::ReadAhead(input)).await.is_ok());
        drop(sender);
        let mut receiver = PartitionReceiver::new(receiver, head);
        if consume {
            let delivered = receiver.batches.next().await.unwrap().unwrap();
            assert_eq!(delivered, expected);
            assert_eq!(budget.permits.available_permits(), MIN_BATCH_CHARGE);
        }
        drop(receiver);
        assert_eq!(budget.permits.available_permits(), MIN_BATCH_CHARGE);
    }
}

#[tokio::test]
async fn zero_column_batches_cannot_bypass_the_shared_budget() {
    let batch = RecordBatch::try_new_with_options(
        Arc::new(open_arrow_access_core::Schema::empty()),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(7)),
    )
    .unwrap();
    let budget = ReadAheadBudget::new(MIN_BATCH_CHARGE);
    let (_first_head, mut first) = watch::channel(false);
    let (second_head, mut second) = watch::channel(false);
    let permit = budget.reserve(&batch, &mut first).await.unwrap();
    let waiting = budget.reserve(&batch, &mut second);
    futures::pin_mut!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    second_head.send_replace(true);
    assert!(waiting.await.is_none());
    assert_eq!(budget.permits.available_permits(), 0);
    drop(permit);
    assert_eq!(budget.permits.available_permits(), MIN_BATCH_CHARGE);
}

#[tokio::test]
async fn head_promotion_progresses_even_when_a_later_file_owns_the_entire_budget() {
    let budget = ReadAheadBudget::new(MIN_BATCH_CHARGE);
    // Model a farther file whose queued output cannot drain before this file.
    let farther_file = budget
        .permits
        .clone()
        .acquire_many_owned(MIN_BATCH_CHARGE as u32)
        .await
        .unwrap();
    let (head, mut worker_head) = watch::channel(false);
    let (sender, receiver) = mpsc::channel(1);
    assert!(sender.send(PartitionOutput::Batch(batch(0))).await.is_ok());
    let (started, decoded) = oneshot::channel();
    let schema = batch(1).schema();
    let metadata = RecordMetadata {
        schema: schema.clone(),
        source: None,
    };
    let mut reader = StreamReader::records(
        stream::once(async move {
            started.send(()).unwrap();
            Ok(batch(1))
        }),
        metadata,
        OrderRequirement::PreserveGlobal,
    );
    let worker_budget = budget.clone();
    let task = tokio::spawn(async move {
        buffer_file(
            &mut reader,
            &sender,
            &mut worker_head,
            &schema,
            &worker_budget,
        )
        .await
        .unwrap()
    });
    decoded.await.unwrap();
    let mut receiver = PartitionReceiver::new(receiver, head);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        receiver.promote();
        assert_eq!(receiver.batches.next().await.unwrap().unwrap(), batch(0));
        assert_eq!(receiver.batches.next().await.unwrap().unwrap(), batch(1));
        assert!(task.await.unwrap());
        assert!(receiver.batches.next().await.is_none());
    })
    .await
    .expect("head promotion must bypass an exhausted read-ahead budget");
    assert_eq!(budget.permits.available_permits(), 0);
    drop(farther_file);
    assert_eq!(budget.permits.available_permits(), MIN_BATCH_CHARGE);
}
