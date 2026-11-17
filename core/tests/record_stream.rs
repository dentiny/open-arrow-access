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

#[tokio::test]
async fn mapped_stream_preserves_contract_without_eager_reads_and_releases_input_on_error() {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
    };

    use open_arrow_access_core::{
        Error, ErrorKind, RecordCapability, StreamReader, into_stream, map_record_stream,
    };
    use open_arrow_access_test_support::DropProbe;

    let batch = batch();
    let schema = batch.schema();
    let polls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe::new(dropped.clone());
    let input_polls = polls.clone();
    let stream = stream::poll_fn(move |_| {
        let _probe = &probe;
        Poll::Ready(match input_polls.fetch_add(1, Ordering::SeqCst) {
            0 => Some(Ok(batch.clone())),
            1 => Some(Err(Error::new(ErrorKind::Timeout, "input", "timed out"))),
            _ => panic!("polled after terminal error"),
        })
    });
    let capability = RecordCapability::sequential(OrderRequirement::PreserveGlobal);
    let input = StreamReader::records(
        stream,
        RecordMetadata {
            schema: schema.clone(),
            source: Some("mapped.arrow".into()),
        },
        capability.ordering,
    );
    let mut reader = map_record_stream(Box::new(input), |reader| {
        into_stream(reader).map(|result| result.map(|batch| batch.slice(0, 1)))
    });
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(reader.capability(), capability);
    assert_eq!(reader.metadata().schema, schema);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "mapped.arrow"
    );
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.operation(), "input");
    assert!(error.to_string().contains("mapped.arrow"));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}
