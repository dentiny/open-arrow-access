use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{OrderRequirement, Reader, RecordMetadata, record_stream_reader};

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int32, false),
            Field::new("right", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![3, 4])),
        ],
    )
    .unwrap()
}

#[rstest::rstest]
#[case::full(None)]
#[case::reordered(Some(vec![1, 0]))]
#[case::empty(Some(vec![]))]
#[tokio::test]
async fn restores_schema_and_field_metadata_for_full_reordered_and_empty_projections(
    #[case] projection: Option<Vec<usize>>,
) {
    let batch = batch();
    let full = Schema::new_with_metadata(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([("unit".into(), "count".into())]))
            })
            .collect::<Vec<_>>(),
        HashMap::from([("version".into(), "fixture".into())]),
    );
    let schema = Arc::new(match &projection {
        Some(indices) => full.project(indices).unwrap(),
        None => full.clone(),
    });
    let mut reader = record_stream_reader(
        stream::iter([Ok(batch.clone())]).then(|item| async move { item }),
        RecordMetadata {
            schema: schema.clone(),
            source: Some("native.xml".into()),
        },
        projection.clone(),
        OrderRequirement::PreserveGlobal,
    );
    assert_eq!(reader.metadata().schema, schema);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "native.xml"
    );
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.schema(), schema);
    assert_eq!(output.num_rows(), 2);
    let indices = projection.unwrap_or_else(|| vec![0, 1]);
    for (output_index, input_index) in indices.into_iter().enumerate() {
        assert!(Arc::ptr_eq(
            output.column(output_index),
            batch.column(input_index)
        ));
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[rstest::rstest]
#[case::metadata(false)]
#[case::batch(true)]
#[tokio::test]
async fn schema_checks_reject_drift_without_reading_ahead(#[case] metadata_matches: bool) {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
    };

    use open_arrow_access_core::{Error, ErrorKind, StreamReader, schema_checked_reader};
    use open_arrow_access_test_support::DropProbe;

    let expected = batch().schema();
    let wrong = Arc::new(
        expected
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([("revision".into(), "different".into())])),
    );
    let metadata = RecordMetadata {
        schema: if metadata_matches {
            expected.clone()
        } else {
            wrong.clone()
        },
        source: Some("partition".into()),
    };
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = DropProbe::new(dropped.clone());
    let stream = stream::poll_fn(move |_| {
        let _keep_alive = &guard;
        observed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(RecordBatch::new_empty(wrong.clone()))))
    });
    let reader = Box::new(StreamReader::records(
        stream,
        metadata,
        OrderRequirement::PreserveGlobal,
    ));
    let result = schema_checked_reader(reader, expected, || {
        Error::new(
            ErrorKind::InvalidInput,
            "custom_schema_check",
            "schema drift",
        )
    });
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let error = if metadata_matches {
        let mut reader = result.unwrap();
        let error = reader.next().await.unwrap_err();
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(
            error.to_string(),
            "custom_schema_check: object \"partition\": schema drift"
        );
        error
    } else {
        result.err().expect("metadata drift was accepted")
    };
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "custom_schema_check");
    assert_eq!(polls.load(Ordering::SeqCst), usize::from(metadata_matches));
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn schema_checks_retain_capabilities_metadata_and_source_errors() {
    use open_arrow_access_core::{
        Error, ErrorKind, RecordCapability, StreamReader, schema_checked_reader,
    };

    let batch = batch();
    let metadata = RecordMetadata {
        schema: batch.schema(),
        source: Some("original".into()),
    };
    let capability = RecordCapability {
        read: true,
        read_with_range: false,
        read_parallel: true,
        list: true,
        ordering: OrderRequirement::PreserveGlobal,
    };
    let source_error = Error::new(ErrorKind::PermissionDenied, "source_read", "denied")
        .with_source(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
    let reader = Box::new(
        StreamReader::records(
            stream::iter([Ok(batch.clone()), Err(source_error)]),
            metadata.clone(),
            capability.ordering,
        )
        .with_capability(capability),
    );
    let mut reader = schema_checked_reader(reader, batch.schema(), || {
        panic!("matching schemas must not construct a mismatch error")
    })
    .unwrap();
    assert_eq!(reader.capability(), capability);
    assert_eq!(reader.metadata().schema, metadata.schema);
    assert_eq!(reader.metadata().source, metadata.source);
    assert_eq!(reader.next().await.unwrap().unwrap(), batch);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "source_read");
    assert_eq!(
        std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::ConnectionReset,
    );
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}
