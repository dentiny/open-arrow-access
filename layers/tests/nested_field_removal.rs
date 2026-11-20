use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int64Array, LargeStringArray, RecordBatch, StructArray, cast::AsArray,
};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions, ReadRequest};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};
use open_arrow_access_test_support::MemoryRecordAccessor;

fn papers() -> RecordBatch {
    let text = Arc::new(LargeStringArray::from(vec![
        Some("prefix"),
        Some("正文"),
        Some("outer hidden"),
        Some("inner hidden"),
        None,
    ]));
    let section = StructArray::new(
        vec![Arc::new(
            Field::new("text.with.dot", DataType::LargeUtf8, true)
                .with_metadata([("source".into(), "paper".into())].into()),
        )]
        .into(),
        vec![text],
        Some(vec![true, true, true, false, true].into()),
    );
    let body = StructArray::new(
        vec![
            Arc::new(Field::new("section", section.data_type().clone(), true)),
            Arc::new(Field::new("annotation", DataType::Int64, false)),
        ]
        .into(),
        vec![
            Arc::new(section),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
        ],
        Some(vec![true, true, false, true, true].into()),
    );
    // Body is not the first source column, exercising dependency remapping.
    RecordBatch::try_from_iter([
        (
            "bibliography",
            Arc::new(LargeStringArray::from(vec!["references"; 5])) as ArrayRef,
        ),
        ("body", Arc::new(body) as ArrayRef),
    ])
    .unwrap()
    .slice(1, 4)
}

#[tokio::test]
async fn move_text_preserves_nulls_siblings_and_projection() {
    let input = papers();
    for (pushdown, projection) in [
        (false, None),
        (true, None),
        (true, Some(vec!["text".into()])),
        (true, Some(vec![])),
    ] {
        let mut layer = SchemaEvolutionLayer::try_new(vec![
            ColumnEvolution::rename_column("body", "record"),
            ColumnEvolution::extract_nested_field(["record", "section", "text.with.dot"], "text"),
            ColumnEvolution::add_column_sql("length", "character_length(text)"),
        ])
        .unwrap();
        if pushdown {
            layer = layer.with_source_schema(input.schema());
        }
        let accessor =
            MemoryRecordAccessor::new(vec![input.clone(), input.slice(0, 0)]).layer(layer);
        let expected_columns = projection.clone().unwrap_or_else(|| {
            ["bibliography", "record", "text", "length"]
                .map(str::to_owned)
                .to_vec()
        });
        let request = ReadRequest::new("papers").with_options(ReadOptions {
            projection,
            ..Default::default()
        });
        let schema = accessor.metadata(request.clone()).await.unwrap().schema;
        assert!(
            schema
                .fields()
                .iter()
                .map(|field| field.name())
                .eq(expected_columns.iter())
        );
        let mut reader = accessor.open(request).await.unwrap();
        for rows in [4, 0] {
            let actual = reader.next().await.unwrap().unwrap();
            assert_eq!(actual.schema(), schema);
            assert_eq!(actual.num_rows(), rows);
            if rows == 0 {
                continue;
            }
            if let Some(text) = actual.column_by_name("text") {
                let text = text.as_string::<i64>();
                assert_eq!(
                    text,
                    &LargeStringArray::from(vec![Some("正文"), None, None, None])
                );
                let original = input
                    .column(1)
                    .as_struct()
                    .column(0)
                    .as_struct()
                    .column(0)
                    .as_string::<i64>();
                assert_eq!(text.value_data().as_ptr(), original.value_data().as_ptr());
                assert_eq!(
                    schema.field_with_name("text").unwrap().metadata()["source"],
                    "paper"
                );
            }
            if let Some(body) = actual.column_by_name("record") {
                let body = body.as_struct();
                assert_eq!(body.nulls(), input.column(1).nulls());
                assert_eq!(body.column(0).as_struct().num_columns(), 0);
                assert!(Arc::ptr_eq(
                    body.column(1),
                    input.column(1).as_struct().column(1)
                ));
                assert_eq!(
                    actual.column_by_name("bibliography"),
                    input.column_by_name("bibliography")
                );
            }
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn remove_nested_field_without_extracting() {
    let accessor = MemoryRecordAccessor::new(vec![papers()]).layer(
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::remove_nested_field([
            "body",
            "section",
            "text.with.dot",
        ])])
        .unwrap(),
    );
    let mut reader = accessor.open(ReadRequest::new("papers")).await.unwrap();
    let actual = reader.next().await.unwrap().unwrap();
    assert_eq!(actual.num_columns(), 2);
    assert_eq!(
        actual
            .column(1)
            .as_struct()
            .column(0)
            .as_struct()
            .num_columns(),
        0
    );
}

#[tokio::test]
async fn rejects_invalid_paths_and_output_collisions() {
    for path in [vec![], vec!["body"], vec!["body", " "]] {
        assert!(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::extract_nested_field(
                path, "text"
            )])
            .is_err()
        );
    }
    for change in [
        ColumnEvolution::extract_nested_field(["body", "section"], "body"),
        ColumnEvolution::remove_nested_field(["body", "missing"]),
        ColumnEvolution::remove_nested_field(["bibliography", "text"]),
    ] {
        let accessor = MemoryRecordAccessor::new(vec![papers()])
            .layer(SchemaEvolutionLayer::try_new(vec![change]).unwrap());
        assert!(accessor.open(ReadRequest::new("papers")).await.is_err());
    }
}
