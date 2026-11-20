use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, LargeStringArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions, ReadRequest};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};
use open_arrow_access_test_support::MemoryRecordAccessor;

fn papers() -> RecordBatch {
    let text = Arc::new(LargeStringArray::from(vec![
        Some("ignored slice prefix"),
        Some("论文正文"),
        Some("hidden by parent"),
        None,
    ]));
    let marker = Arc::new(Int64Array::from(vec![0, 1, 2, 3]));
    let body = StructArray::new(
        vec![
            Arc::new(Field::new("text", DataType::LargeUtf8, true)),
            Arc::new(Field::new("annotation", DataType::Int64, false)),
        ]
        .into(),
        vec![text, marker],
        Some(vec![true, true, false, true].into()),
    );
    let bibliography = Arc::new(LargeStringArray::from(vec!["references"; 4]));
    RecordBatch::try_from_iter([
        ("body", Arc::new(body) as ArrayRef),
        ("bibliography", bibliography as ArrayRef),
    ])
    .unwrap()
    .slice(1, 3)
}

#[tokio::test]
async fn move_body_text_preserves_nulls_and_projection() {
    // Reorder source fields so dependency projection must rebind the body slot.
    let input = papers().project(&[1, 0]).unwrap();
    let changes = vec![
        ColumnEvolution::extract_nested_field(["body", "text"], "text"),
        ColumnEvolution::add_column_sql("length", "character_length(text)"),
    ];
    for pushdown in [false, true] {
        for projection in [None, Some(vec!["text".into()]), Some(vec![])] {
            let mut layer = SchemaEvolutionLayer::try_new(changes.clone()).unwrap();
            if pushdown {
                layer = layer.with_source_schema(input.schema());
            }
            let accessor = MemoryRecordAccessor::new(vec![input.clone()]).layer(layer);
            let request = ReadRequest::new("papers").with_options(ReadOptions {
                projection: projection.clone(),
                ..Default::default()
            });
            let metadata = accessor.metadata(request.clone()).await.unwrap();
            let mut reader = accessor.open(request).await.unwrap();
            let actual = reader.next().await.unwrap().unwrap();
            assert_eq!(actual.schema(), metadata.schema);
            assert_eq!(actual.num_rows(), 3);
            if projection.as_ref().is_none_or(|names| !names.is_empty()) {
                let text = actual.column_by_name("text").unwrap();
                assert_eq!(
                    text.as_ref(),
                    &LargeStringArray::from(vec![Some("论文正文"), None, None]) as &dyn Array
                );
            }
            if projection.is_none() {
                let body = actual
                    .column_by_name("body")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                assert!(body.column_by_name("text").is_none());
                assert_eq!(body.nulls(), input.column_by_name("body").unwrap().nulls());
                let original = input
                    .column_by_name("body")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                assert!(Arc::ptr_eq(body.column(0), original.column(1)));
                assert_eq!(
                    actual.column_by_name("bibliography"),
                    input.column_by_name("bibliography")
                );
            }
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn deep_literal_paths_keep_empty_structs_metadata_and_row_counts() {
    let field = Arc::new(Field::new("text.with.dot", DataType::LargeUtf8, true));
    let inner = StructArray::new(
        vec![field].into(),
        vec![Arc::new(LargeStringArray::from(vec!["x", "y"]))],
        None,
    );
    let inner_field = Arc::new(
        Field::new("nested", inner.data_type().clone(), true)
            .with_metadata([("custom".into(), "kept".into())].into()),
    );
    let outer = StructArray::new(
        vec![inner_field].into(),
        vec![Arc::new(inner)],
        Some(vec![false, true].into()),
    );
    let input = RecordBatch::try_from_iter([("body", Arc::new(outer) as ArrayRef)]).unwrap();
    let accessor = MemoryRecordAccessor::new(vec![input.clone(), input.slice(0, 0)]).layer(
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::remove_nested_field([
            "body",
            "nested",
            "text.with.dot",
        ])])
        .unwrap(),
    );
    let mut reader = accessor.open(ReadRequest::new("papers")).await.unwrap();
    for rows in [2, 0] {
        let actual = reader.next().await.unwrap().unwrap();
        assert_eq!(actual.num_rows(), rows);
        let body = actual
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let nested = body
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(nested.len(), rows);
        assert_eq!(nested.num_columns(), 0);
        let DataType::Struct(fields) = body.data_type() else {
            panic!("struct")
        };
        assert_eq!(fields[0].metadata()["custom"], "kept");
    }
}

#[tokio::test]
async fn rejects_invalid_paths_and_use_after_removal() {
    for path in [vec![], vec!["body"], vec!["body", " "]] {
        assert!(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::remove_nested_field(path)])
                .is_err()
        );
    }
    for changes in [
        vec![ColumnEvolution::extract_nested_field(
            ["body", "text"],
            "body",
        )],
        vec![ColumnEvolution::extract_nested_field(
            ["body", "missing"],
            "text",
        )],
        vec![ColumnEvolution::remove_nested_field(["missing", "text"])],
        vec![ColumnEvolution::remove_nested_field(["body", "missing"])],
        vec![ColumnEvolution::remove_nested_field([
            "bibliography",
            "text",
        ])],
        vec![
            ColumnEvolution::remove_nested_field(["body", "text"]),
            ColumnEvolution::add_column_sql(
                "text",
                "CASE WHEN body IS NULL THEN NULL ELSE body.text END",
            ),
        ],
    ] {
        let accessor = MemoryRecordAccessor::new(vec![papers()])
            .layer(SchemaEvolutionLayer::try_new(changes).unwrap());
        assert!(accessor.open(ReadRequest::new("papers")).await.is_err());
    }
}

#[tokio::test]
async fn nested_extraction_combines_all_parent_nulls_and_preserves_value_buffers() {
    let text = Arc::new(LargeStringArray::from(vec![
        "hidden outer",
        "hidden inner",
        "visible",
    ]));
    let inner = StructArray::new(
        vec![Arc::new(Field::new("text", DataType::LargeUtf8, false))].into(),
        vec![text.clone()],
        Some(vec![true, false, true].into()),
    );
    let outer = StructArray::new(
        vec![Arc::new(Field::new(
            "inner",
            inner.data_type().clone(),
            true,
        ))]
        .into(),
        vec![Arc::new(inner)],
        Some(vec![false, true, true].into()),
    );
    let input = RecordBatch::try_from_iter([("body", Arc::new(outer) as ArrayRef)]).unwrap();
    let accessor = MemoryRecordAccessor::new(vec![input]).layer(
        SchemaEvolutionLayer::try_new(vec![
            ColumnEvolution::rename_column("body", "record"),
            ColumnEvolution::extract_nested_field(["record", "inner", "text"], "text"),
        ])
        .unwrap(),
    );
    let mut reader = accessor.open(ReadRequest::new("papers")).await.unwrap();
    assert!(
        reader
            .metadata()
            .schema
            .field_with_name("text")
            .unwrap()
            .is_nullable()
    );
    let output = reader.next().await.unwrap().unwrap();
    let actual = output
        .column_by_name("text")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    assert_eq!(
        actual,
        &LargeStringArray::from(vec![None, None, Some("visible")])
    );
    assert_eq!(actual.value_data().as_ptr(), text.value_data().as_ptr());
}
