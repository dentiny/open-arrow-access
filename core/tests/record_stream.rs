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
}

#[tokio::test]
async fn mapped_reader_uses_explicit_capabilities_and_drops_state_on_transform_error() {
    use open_arrow_access_core::{
        Capability, Error, ErrorKind, RecordCapability, StreamReader, map_record_reader,
    };
    use open_arrow_access_test_support::DropProbe;
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
    };

    let input = batch();
    let input_schema = input.schema();
    let output_schema = Arc::new(input_schema.project(&[]).unwrap());
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let input_dropped = Arc::new(AtomicBool::new(false));
    let input_probe = DropProbe::new(input_dropped.clone());
    let input = stream::poll_fn(move |_| {
        let _keep_alive = &input_probe;
        observed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(input.clone())))
    });
    let input_capability = Capability {
        read: true,
        read_with_range: false,
        read_parallel: true,
        list: false,
    }
    .with_ordering(OrderRequirement::PreserveGlobal);
    let input = StreamReader::records(
        input,
        RecordMetadata {
            schema: input_schema,
            source: Some("mapped.json".into()),
        },
        input_capability.ordering,
    )
    .with_capability(input_capability);
    let output_capability = RecordCapability::sequential(input_capability.ordering);
    let state_dropped = Arc::new(AtomicBool::new(false));
    let state_probe = DropProbe::new(state_dropped.clone());
    let mut calls = 0;
    let mut reader = map_record_reader(
        Box::new(input),
        output_schema.clone(),
        output_capability,
        move |batch| {
            let _keep_alive = &state_probe;
            calls += 1;
            if calls == 2 {
                return Err(Error::new(
                    ErrorKind::Decode,
                    "map_fixture",
                    "original error",
                ));
            }
            Ok(batch.project(&[]).unwrap())
        },
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.capability(), output_capability);
    assert_eq!(reader.metadata().schema, output_schema);
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "mapped.json"
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 2);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.operation(), "map_fixture");
    assert_eq!(error.to_string(), "map_fixture: original error");
    assert!(input_dropped.load(Ordering::SeqCst));
    assert!(state_dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}
