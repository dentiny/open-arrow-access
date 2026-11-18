use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    ErrorKind, OrderRequirement, Reader, RecordBatch, RecordMetadata, ScanPartition, ScanPlan,
    StreamReader, scan_partition,
};

fn metadata(tag: &str) -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(
            Schema::new(vec![Field::new("n", DataType::Int32, false)])
                .with_metadata([("tag".to_owned(), tag.to_owned())].into()),
        ),
        source: None,
    }
}

#[tokio::test]
async fn sequential_scan_rejects_metadata_and_batch_schema_drift() {
    for wrong_metadata in [false, true] {
        let expected = metadata("scan");
        let wrong = metadata("different");
        let partition = scan_partition(async move {
            let (metadata, batches) = if wrong_metadata {
                // Even an empty partition must agree with the scan schema.
                (wrong, vec![])
            } else {
                (
                    metadata("scan"),
                    vec![Ok(RecordBatch::new_empty(wrong.schema))],
                )
            };
            Ok(StreamReader::records(
                stream::iter(batches),
                metadata,
                OrderRequirement::PreserveGlobal,
            ))
        });
        let opened = Arc::new(AtomicBool::new(false));
        let observed = opened.clone();
        let later: ScanPartition = Box::pin(async move {
            observed.store(true, Ordering::SeqCst);
            unreachable!("a schema error must not open later partitions")
        });
        let mut reader = ScanPlan::new(expected.clone(), vec![partition, later]).into_reader();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert_eq!(reader.metadata().schema, expected.schema);
        assert!(reader.next().await.unwrap().is_none());
        assert!(!opened.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn sequential_scan_opens_on_demand_and_preserves_order_and_duplicates() {
    let schema = metadata("scan");
    let opened = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let discovered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let partition_discoveries = discovered.clone();
    let partition_opens = opened.clone();
    let partition_schema = schema.clone();
    let partitions = [None, Some(2), Some(2), Some(4)]
        .into_iter()
        .map(move |value| {
            partition_discoveries.fetch_add(1, Ordering::SeqCst);
            let opened = partition_opens.clone();
            let mut metadata = partition_schema.clone();
            metadata.source = Some("partition".into());
            scan_partition(async move {
                opened.fetch_add(1, Ordering::SeqCst);
                let batches = value.map(|n| {
                    Ok(RecordBatch::try_new(
                        metadata.schema.clone(),
                        vec![Arc::new(Int32Array::from(vec![n]))],
                    )
                    .unwrap())
                });
                Ok(StreamReader::records(
                    stream::iter(batches),
                    metadata,
                    OrderRequirement::PreserveGlobal,
                ))
            })
        });
    let mut reader = ScanPlan::from_iter(schema.clone(), partitions).into_reader();
    assert_eq!(discovered.load(Ordering::SeqCst), 0);
    assert_eq!(opened.load(Ordering::SeqCst), 0);
    for (count, value) in [(2, 2), (3, 2), (4, 4)] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema(), schema.schema);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            value
        );
        assert_eq!(opened.load(Ordering::SeqCst), count);
        assert_eq!(discovered.load(Ordering::SeqCst), count);
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}
