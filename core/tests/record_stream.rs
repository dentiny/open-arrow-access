use std::{
    collections::HashMap,
    error::Error as _,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, StreamExt, stream};
use open_arrow_access_core::{
    Error, ErrorKind, Reader, RecordMetadata, Result, record_stream_reader,
};
use open_arrow_access_test_support::DropProbe;

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

fn tracked_stream(
    result: Result<RecordBatch>,
) -> (impl Stream<Item = Result<RecordBatch>>, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = DropProbe::new(dropped.clone());
    let stream = stream::iter([result]).map(move |item| {
        let _keep_alive = &guard;
        item
    });
    (stream, dropped)
}

#[tokio::test]
async fn restores_schema_and_field_metadata_for_full_reordered_and_empty_projections() {
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
    for projection in [None, Some(vec![1, 0]), Some(vec![])] {
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
}

#[tokio::test]
async fn source_and_projection_errors_terminate_and_release_the_stream() {
    for source_error in [false, true] {
        let batch = batch();
        let metadata = RecordMetadata {
            schema: batch.schema(),
            source: None,
        };
        let result = if source_error {
            Err(
                Error::new(ErrorKind::PermissionDenied, "native_read", "source")
                    .with_source(io::Error::new(io::ErrorKind::PermissionDenied, "cause")),
            )
        } else {
            Ok(batch)
        };
        let (stream, dropped) = tracked_stream(result);
        let mut reader = record_stream_reader(stream, metadata, Some(vec![9]));
        let error = reader.next().await.unwrap_err();
        if source_error {
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert_eq!(error.operation(), "native_read");
            assert_eq!(error.source().unwrap().to_string(), "cause");
        } else {
            assert_eq!(error.kind(), ErrorKind::Decode);
            assert_eq!(error.operation(), "project_batch");
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn schema_mismatch_is_a_decode_error_and_releases_the_stream() {
    let (stream, dropped) = tracked_stream(Ok(batch()));
    let metadata = RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    };
    let mut reader = record_stream_reader(stream, metadata, None);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "project_batch");
    assert!(dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
}
