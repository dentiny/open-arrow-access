use std::{collections::HashMap, sync::Arc};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{Reader, RecordMetadata, record_stream_reader};

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
