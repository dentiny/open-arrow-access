use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use datafusion::{
    error::DataFusionError,
    logical_expr::{ColumnarValue, Expr, Volatility, create_udf},
    prelude::{col, lit},
};

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use open_arrow_access_core::{
    Accessor, AccessorExt, ErrorKind, ObjectRef, OrderRequirement, ReadOptions, ReadRequest,
    RecordReader, Result,
};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};
use open_arrow_access_test_support::MemoryRecordAccessor as MemoryAccessor;

fn layer(name: &str, data_type: DataType) -> SchemaEvolutionLayer {
    SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type(name, data_type)]).unwrap()
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

fn dictionary(value: DataType) -> DataType {
    DataType::Dictionary(Box::new(DataType::Int8), Box::new(value))
}

fn union(types: Vec<DataType>) -> DataType {
    let ids = 0..types.len() as i8;
    DataType::Union(
        UnionFields::try_new(ids, types.into_iter().map(item)).unwrap(),
        UnionMode::Sparse,
    )
}

fn counted_column(name: &str, data_type: DataType, calls: Arc<AtomicUsize>) -> Expr {
    create_udf(
        "observe",
        vec![data_type.clone()],
        data_type,
        Volatility::Stable,
        Arc::new(move |args| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    )
    .call(vec![col(name)])
}

trait EvolutionTestExt: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + Sized {
    async fn open_evolved(
        self,
        changes: Vec<ColumnEvolution>,
        projection: Option<Vec<String>>,
    ) -> Result<Box<dyn RecordReader>> {
        self.open_layer(SchemaEvolutionLayer::try_new(changes)?, projection)
            .await
    }

    async fn open_layer(
        self,
        layer: SchemaEvolutionLayer,
        projection: Option<Vec<String>>,
    ) -> Result<Box<dyn RecordReader>> {
        self.layer(layer)
            .open(ReadRequest::new("test").with_options(ReadOptions {
                projection,
                ..Default::default()
            }))
            .await
    }

    async fn open_cast(self, target: DataType) -> Result<Box<dyn RecordReader>> {
        self.open_layer(layer("value", target), None).await
    }
}

impl EvolutionTestExt for MemoryAccessor {}

fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("value", array)]).unwrap()
}

fn column_names(batch: &RecordBatch) -> Vec<&str> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect()
}

#[tokio::test]
async fn projection_keeps_cast_dependencies_and_zero_column_row_counts() {
    let input = RecordBatch::try_from_iter([
        (
            "visible",
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec!["3", "4"])) as ArrayRef,
        ),
    ])
    .unwrap();
    for names in [vec!["value", "visible"], vec!["visible"], vec![]] {
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .open_evolved(
                vec![ColumnEvolution::change_data_type("value", DataType::Int32)],
                Some(names.iter().map(|name| (*name).into()).collect()),
            )
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(column_names(&output), names);
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.schema(), reader.metadata().schema);
    }
    // Cast-only plans must still read and evaluate columns hidden by projection.
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["bad"])))])
        .open_evolved(
            vec![ColumnEvolution::change_data_type("value", DataType::Int32)],
            Some(vec![]),
        )
        .await
        .unwrap();
    assert!(reader.next().await.is_err());
}

#[tokio::test]
async fn identity_casts_share_batches() {
    let input = batch(Arc::new(StringArray::from(vec!["42"])));
    for evolution in [
        SchemaEvolutionLayer::default(),
        layer("value", DataType::Utf8),
        SchemaEvolutionLayer::try_new(vec![ColumnEvolution::rename_column("value", "value")])
            .unwrap(),
    ] {
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .layer(evolution)
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert!(Arc::ptr_eq(output.schema_ref(), input.schema_ref()));
        assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    }
}

#[tokio::test]
async fn cast_validation_rejects_unsupported_and_unsafe_types_before_reading() {
    let decimal = |scale| DataType::Decimal256(76, scale);
    let fields = vec![
        Field::new("a", decimal(0), true),
        Field::new("b", decimal(60), true),
    ];
    for (from, to, rejected) in [
        (
            DataType::Int32,
            DataType::Struct(vec![item(DataType::Int32)].into()),
            true,
        ),
        (decimal(0), decimal(60), true),
        (decimal(0), decimal(52), true),
        (
            DataType::Decimal128(38, 38),
            DataType::Decimal128(38, -128),
            true,
        ),
        (
            DataType::List(item(dictionary(decimal(0)))),
            DataType::List(item(dictionary(decimal(60)))),
            true,
        ),
        (
            union(vec![decimal(0), decimal(60)]),
            dictionary(decimal(60)),
            true,
        ),
        (decimal(0), decimal(51), false),
        (
            DataType::Struct(fields.clone().into()),
            DataType::Struct(fields.into_iter().rev().collect()),
            false,
        ),
        (
            dictionary(dictionary(union(vec![decimal(0), decimal(60)]))),
            dictionary(decimal(60)),
            false,
        ),
    ] {
        let source = MemoryAccessor::new(vec![batch(arrow_array::new_empty_array(&from))]);
        let reads = source.batches_read.clone();
        let result = source.open_cast(to.clone()).await;
        if rejected {
            let error = result.err().unwrap();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
            assert_eq!(error.operation(), "plan_cast");
            let message = error.to_string();
            assert!(message.contains("cannot cast"), "{error}");
            assert!(message.contains("value"), "{error}");
            assert!(message.contains(&from.to_string()), "{error}");
            assert!(message.contains(&to.to_string()), "{error}");
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn added_expressions_reject_unsafe_decimal_casts_even_when_hidden() {
    use datafusion::logical_expr::{Cast, TryCast};
    let target = DataType::Decimal256(76, 60);
    for expression in [
        Expr::Cast(Cast::new(Box::new(col("value")), target.clone())),
        Expr::TryCast(TryCast::new(Box::new(col("value")), target.clone())),
    ] {
        let source = MemoryAccessor::new(vec![batch(arrow_array::new_null_array(
            &DataType::Decimal256(76, 0),
            1,
        ))]);
        let reads = source.batches_read.clone();
        let error = source
            .open_evolved(
                vec![ColumnEvolution::add_column(
                    "scaled",
                    target.clone(),
                    expression,
                )],
                Some(vec![]),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("cannot cast"), "{error}");
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn added_expressions_reject_run_end_casts_before_reading() {
    use datafusion::logical_expr::{Cast, TryCast};
    let encoded = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int16, false)),
        item(DataType::Int64),
    );
    for target in [
        encoded.clone(),
        DataType::Struct(vec![item(encoded)].into()),
    ] {
        for expression in [
            Expr::Cast(Cast::new(Box::new(col("value")), target.clone())),
            Expr::TryCast(TryCast::new(Box::new(col("value")), target.clone())),
        ] {
            let source =
                MemoryAccessor::new(vec![batch(Arc::new(arrow_array::NullArray::new(32768)))]);
            let reads = source.batches_read.clone();
            let error = source
                .open_evolved(
                    vec![ColumnEvolution::add_column(
                        "encoded",
                        target.clone(),
                        expression,
                    )],
                    Some(vec![]),
                )
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert!(error.to_string().contains("cannot cast"), "{error}");
            assert_eq!(reads.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn timestamp_fallback_preserves_values_nulls_and_errors() {
    use arrow_schema::TimeUnit;
    let cases = [
        ("2000-05-31T04:00Z", "2000-05-31T04:00:00Z"),
        ("2000-05-31 06:00+02:00", "2000-05-31T04:00:00Z"),
        ("2000-05-31t04:00z", "2000-05-31T04:00:00Z"),
        ("2000-05-31T04:00", "2000-05-31T04:00:00"),
        (
            "2000-05-31T04:00:01.123456789Z",
            "2000-05-31T04:00:01.123456789Z",
        ),
    ];
    let input = StringArray::from(
        cases
            .iter()
            .map(|(input, _)| Some(*input))
            .chain([None])
            .collect::<Vec<_>>(),
    );
    let expected = StringArray::from(
        cases
            .iter()
            .map(|(_, value)| Some(*value))
            .chain([None])
            .collect::<Vec<_>>(),
    );
    // Exercise each string layout and timezone context.
    for (source_type, unit, timezone) in [
        (DataType::Utf8, TimeUnit::Second, None),
        (DataType::LargeUtf8, TimeUnit::Millisecond, Some("UTC")),
        (
            DataType::Utf8View,
            TimeUnit::Microsecond,
            Some("America/New_York"),
        ),
    ] {
        let target = DataType::Timestamp(unit, timezone.map(Into::into));
        let input = arrow_cast::cast(&input, &source_type).unwrap();
        let expected = arrow_cast::cast(&expected, &target).unwrap();
        let mut reader = MemoryAccessor::new(vec![batch(input)])
            .open_cast(target)
            .await
            .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().column(0), &expected);
    }
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec![
        "2000-02-30T04:00Z",
    ])))])
    .open_cast(DataType::Timestamp(TimeUnit::Second, None))
    .await
    .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "cast");
}

#[tokio::test]
async fn renames_preserve_column_identity_through_swaps_and_name_reuse() {
    let input = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let rename = |from: &str, to: &str| ColumnEvolution::rename_column(from, to);
    for (changes, names, sources) in [
        (
            vec![rename("a", "tmp"), rename("b", "a"), rename("tmp", "b")],
            ["a", "b"],
            [1, 0],
        ),
        (
            vec![
                ColumnEvolution::remove_column("b"),
                rename("a", "b"),
                ColumnEvolution::add_column("a", DataType::Int64, col("b")),
            ],
            ["b", "a"],
            [0, 0],
        ),
    ] {
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .open_evolved(
                changes,
                Some(names.iter().map(|name| (*name).into()).collect()),
            )
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(column_names(&output), names);
        for (column, source) in output.columns().iter().zip(sources) {
            assert!(Arc::ptr_eq(column, input.column(source)));
        }
    }
}

#[tokio::test]
async fn evolution_combines_all_operations_and_expressions_see_previous_changes() {
    let source_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("remove", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true)
                .with_metadata([("unit".into(), "count".into())].into()),
            Field::new("keep", DataType::Int32, false),
        ],
        [("dataset".into(), "evolution".into())].into(),
    ));
    let input = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(StringArray::from(vec![Some("42"), None])),
            Arc::new(Int32Array::from(vec![3, 4])),
        ],
    )
    .unwrap();
    for (known_schema, projection) in [
        (false, None),
        (true, None),
        (true, Some(vec!["copied".into(), "count".into()])),
        (true, Some(vec![])),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let copied = calls.clone();
        let changes = vec![
            ColumnEvolution::rename_column("value", "count"),
            ColumnEvolution::change_data_type("count", DataType::Int32),
            ColumnEvolution::add_column(
                "added",
                DataType::Int32,
                counted_column("count", DataType::Int32, seen),
            ),
            ColumnEvolution::remove_column("remove"),
            ColumnEvolution::rename_column("added", "middle"),
            ColumnEvolution::add_column(
                "copied",
                DataType::Int32,
                counted_column("middle", DataType::Int32, copied) + col("keep"),
            ),
        ];
        let source = MemoryAccessor::new(vec![input.clone(), input.slice(0, 0), input.slice(1, 1)]);
        let requested = source.projections.clone();
        let mut layer = SchemaEvolutionLayer::try_new(changes).unwrap();
        if known_schema {
            layer = layer.with_source_schema(source_schema.clone());
        }
        let mut reader = source.open_layer(layer, projection.clone()).await.unwrap();
        assert_eq!(
            requested.lock().unwrap().last().cloned().unwrap(),
            known_schema.then(|| vec!["value".into(), "keep".into()])
        );
        let schema = &reader.metadata().schema;
        assert_eq!(reader.metadata().source, Some(ObjectRef::new("test")));
        assert_eq!(reader.capability().ordering, OrderRequirement::Reorderable);
        assert_eq!(schema.metadata(), source_schema.metadata());
        if let Ok(count) = schema.field_with_name("count") {
            assert_eq!(count.metadata(), source_schema.field(1).metadata());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let expected_names = projection.unwrap_or_else(|| {
            ["count", "keep", "middle", "copied"]
                .map(String::from)
                .to_vec()
        });
        for values in [vec![Some(42), None], vec![], vec![None]] {
            let output = reader.next().await.unwrap().unwrap();
            assert_eq!(output.num_rows(), values.len());
            assert!(Arc::ptr_eq(output.schema_ref(), &reader.metadata().schema));
            assert_eq!(column_names(&output), expected_names);
            for name in ["count", "middle", "copied"] {
                if let Some(column) = output.column_by_name(name) {
                    let expected: Vec<_> = values
                        .iter()
                        .map(|value| {
                            value.map(|value| value + if name == "copied" { 3 } else { 0 })
                        })
                        .collect();
                    assert_eq!(column.to_data(), Int32Array::from(expected).to_data());
                }
            }
            if values.len() == 2
                && let Some(column) = output.column_by_name("keep")
            {
                assert!(Arc::ptr_eq(column, input.column(2)));
            }
        }
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }
}

#[test]
fn evolution_rejects_invalid_names_and_types() {
    for change in [
        ColumnEvolution::add_column_sql("x", " "),
        ColumnEvolution::add_column_sql(" ", "1"),
        ColumnEvolution::remove_column(""),
        ColumnEvolution::change_data_type("", DataType::Int32),
        ColumnEvolution::add_column("", DataType::Int32, lit(1i32)),
        ColumnEvolution::change_data_type(
            "a",
            DataType::Time32(arrow_schema::TimeUnit::Nanosecond),
        ),
        ColumnEvolution::add_column(
            "bad",
            DataType::Time32(arrow_schema::TimeUnit::Nanosecond),
            lit(1i32),
        ),
        ColumnEvolution::add_column(
            "bad",
            DataType::List(item(DataType::Time64(arrow_schema::TimeUnit::Second))),
            lit(1i32),
        ),
        ColumnEvolution::add_column(
            "bad",
            DataType::RunEndEncoded(item(DataType::UInt32), item(DataType::Int32)),
            lit(1i32),
        ),
        ColumnEvolution::rename_column("a", ""),
        ColumnEvolution::rename_column("", "a"),
    ] {
        assert_eq!(
            SchemaEvolutionLayer::try_new(vec![change])
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn evolution_checks_udf_results_and_preserves_errors_when_hidden() {
    for failure in ["type", "length", "error"] {
        let expression = create_udf(
            "broken",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Stable,
            Arc::new(move |_| match failure {
                "type" => Ok(ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2])))),
                "length" => Ok(ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])))),
                _ => Err(DataFusionError::Execution("custom failure".into())),
            }),
        )
        .call(vec![col("value")]);
        for change in [
            ColumnEvolution::add_column("added", DataType::Int32, expression.clone()),
            ColumnEvolution::add_column_expr("added", expression),
        ] {
            let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1, 2])))]);
            let reads = source.batches_read.clone();
            let mut reader = source
                .open_evolved(vec![change], Some(vec![]))
                .await
                .unwrap();
            let error = reader.next().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Decode);
            assert!(error.to_string().contains("added"));
            if failure == "error" {
                assert!(std::error::Error::source(&error).is_some());
                assert!(error.to_string().contains("custom failure"));
            }
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(reads.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn evolution_adds_nullable_and_nested_arrays_without_casting_them() {
    for data_type in [
        DataType::List(item(DataType::Utf8)),
        DataType::Struct(vec![item(DataType::Int32)].into()),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            item(DataType::Int32),
        ),
    ] {
        let expected = arrow_array::new_null_array(&data_type, 2);
        let input = RecordBatch::try_from_iter([("value", expected.clone())]).unwrap();
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .open_evolved(
                vec![
                    ColumnEvolution::add_column("added", data_type.clone(), col("value")),
                    ColumnEvolution::add_column(
                        "udf",
                        data_type.clone(),
                        counted_column("value", data_type.clone(), Arc::default()),
                    ),
                ],
                None,
            )
            .await
            .unwrap();
        assert!(reader.metadata().schema.field(1).is_nullable());
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.column(1).data_type(), &data_type);
        assert!(Arc::ptr_eq(output.column(1), &expected));
        assert!(Arc::ptr_eq(output.column(2), &expected));
        assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    }
}

#[tokio::test]
async fn casts_on_added_columns_execute_in_vector_order() {
    let input = batch(Arc::new(StringArray::from(vec!["001"])));
    let mut reader = MemoryAccessor::new(vec![input])
        .open_evolved(
            vec![
                ColumnEvolution::add_column("added", DataType::Utf8, col("value")),
                ColumnEvolution::change_data_type("added", DataType::Int32),
                ColumnEvolution::rename_column("added", "final"),
                ColumnEvolution::change_data_type("final", DataType::Utf8),
            ],
            Some(vec!["final".into()]),
        )
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    // The intermediate numeric cast must execute, rather than collapse to Utf8 -> Utf8.
    assert_eq!(
        output.column(0).to_data(),
        StringArray::from(vec!["1"]).to_data()
    );
}

#[tokio::test]
async fn evolution_allows_add_remove_add_and_evaluates_removed_additions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1])))])
        .open_evolved(
            vec![
                ColumnEvolution::add_column(
                    "added",
                    DataType::Int32,
                    counted_column("value", DataType::Int32, seen),
                ),
                ColumnEvolution::remove_column("added"),
                ColumnEvolution::add_column("added", DataType::Int64, lit(2i64)),
                ColumnEvolution::remove_column("value"),
            ],
            None,
        )
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(output.num_columns(), 1);
    assert_eq!(output.schema().field(0).name(), "added");
    assert_eq!(
        output.column(0).to_data(),
        Int64Array::from(vec![2]).to_data()
    );
}

#[tokio::test]
async fn evolution_checks_names_at_each_step_before_reading() {
    for changes in [
        vec![
            ColumnEvolution::rename_column("a", "c"),
            ColumnEvolution::add_column("copy", DataType::Int32, col("a")),
        ],
        vec![
            ColumnEvolution::remove_column("a"),
            ColumnEvolution::add_column("copy", DataType::Int32, col("a")),
        ],
        vec![ColumnEvolution::remove_column("missing")],
        vec![ColumnEvolution::rename_column("missing", "renamed")],
        vec![ColumnEvolution::change_data_type(
            "missing",
            DataType::Int32,
        )],
        vec![ColumnEvolution::add_column("a", DataType::Int32, lit(1i32))],
        vec![
            ColumnEvolution::rename_column("a", "c"),
            ColumnEvolution::add_column("c", DataType::Int32, lit(1i32)),
        ],
        vec![
            ColumnEvolution::rename_column("a", "b"),
            ColumnEvolution::remove_column("b"),
        ],
        vec![
            ColumnEvolution::add_column("c", DataType::Int32, lit(1i32)),
            ColumnEvolution::add_column("c", DataType::Int32, lit(1i32)),
        ],
    ] {
        let input = RecordBatch::try_from_iter([
            ("a", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            ("b", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
        ])
        .unwrap();
        let source = MemoryAccessor::new(vec![input]);
        let reads = source.batches_read.clone();
        let error = source
            .open_evolved(changes, Some(vec![]))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn evolution_preserves_runtime_order_between_casts_and_expressions() {
    for cast_first in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let mut changes = vec![
            ColumnEvolution::add_column(
                "marker",
                DataType::Utf8,
                counted_column("value", DataType::Utf8, seen),
            ),
            ColumnEvolution::change_data_type("value", DataType::Int32),
        ];
        if cast_first {
            changes.reverse();
        }
        let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["bad"])))])
            .open_evolved(changes, Some(vec![]))
            .await
            .unwrap();
        assert!(reader.next().await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!cast_first));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn identity_renames_preserve_projection_pushdown_and_dependencies() {
    let input = RecordBatch::try_from_iter([
        ("value", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ("keep", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
        (
            "payload",
            Arc::new(StringArray::from(vec!["unneeded"])) as ArrayRef,
        ),
    ])
    .unwrap();
    for (names, cast, expected) in [
        (vec!["value"], false, vec!["value"]),
        (vec![], false, vec!["value"]),
        (vec!["keep"], true, vec!["keep", "value"]),
        (vec!["value"], true, vec!["value", "keep"]),
    ] {
        let source = MemoryAccessor::new(vec![input.clone()]);
        let requested = source.projections.clone();
        let mut changes = vec![ColumnEvolution::rename_column("value", "value")];
        if cast {
            changes.push(ColumnEvolution::change_data_type("keep", DataType::Int64));
        }
        let mut reader = source
            .open_evolved(
                changes,
                Some(names.iter().map(|name| (*name).into()).collect()),
            )
            .await
            .unwrap();
        let expected: Vec<String> = expected.into_iter().map(String::from).collect();
        assert_eq!(
            requested.lock().unwrap().last().cloned().unwrap(),
            Some(expected)
        );
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(column_names(&output), names);
        if let Some(keep) = output.column_by_name("keep") {
            assert_eq!(keep.data_type(), &DataType::Int64);
        }
    }
}

#[tokio::test]
async fn scalar_expressions_preserve_rows_with_no_source_columns() {
    let input = batch(Arc::new(Int32Array::from(vec![1, 2, 3])));
    let source = MemoryAccessor::new(vec![input.clone(), input.slice(0, 0)]);
    let requested = source.projections.clone();
    let layer = SchemaEvolutionLayer::try_new(vec![
        ColumnEvolution::remove_column("value"),
        ColumnEvolution::add_column(
            "value",
            DataType::Int64,
            datafusion::functions::core::expr_fn::coalesce(vec![lit(3i32), lit(4i64)]),
        ),
    ])
    .unwrap()
    .with_source_schema(input.schema());
    let mut reader = source.open_layer(layer, None).await.unwrap();
    assert_eq!(
        requested.lock().unwrap().last().cloned().unwrap(),
        Some(vec![])
    );
    for rows in [3, 0] {
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_rows(), rows);
        assert_eq!(
            output.column(0).to_data(),
            Int64Array::from(vec![3; rows]).to_data()
        );
    }
}

#[tokio::test]
async fn invalid_expression_types_and_source_schemas_fail_before_reading() {
    let input = batch(Arc::new(Int32Array::from(vec![1])));
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    for (change, schema, kind) in [
        (
            ColumnEvolution::add_column("copy", DataType::Int64, col("value")),
            None,
            ErrorKind::InvalidInput,
        ),
        (
            ColumnEvolution::add_column("value", DataType::Int32, lit(1i32)),
            Some(input.schema()),
            ErrorKind::InvalidInput,
        ),
        (
            ColumnEvolution::add_column("copy", DataType::Int64, col("value")),
            Some(wrong_schema),
            ErrorKind::Decode,
        ),
    ] {
        let source = MemoryAccessor::new(vec![input.clone()]);
        let reads = source.batches_read.clone();
        let mut layer = SchemaEvolutionLayer::try_new(vec![change]).unwrap();
        if let Some(schema) = schema {
            layer = layer.with_source_schema(schema);
        }
        let error = source.open_layer(layer, Some(vec![])).await.err().unwrap();
        assert_eq!(error.kind(), kind);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn hidden_expression_chain_releases_arrays_after_their_last_use() {
    for known_schema in [false, true] {
        let arrays = Arc::new(std::sync::Mutex::new(
            Vec::<std::sync::Weak<dyn Array>>::new(),
        ));
        let observed = arrays.clone();
        let expression = create_udf(
            "track_live_arrays",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Stable,
            Arc::new(move |args| {
                let mut arrays = observed.lock().unwrap();
                // When the third expression runs, the first result must be gone.
                if arrays.len() == 2 {
                    assert_eq!(arrays[0].strong_count(), 0);
                }
                let input = args[0].clone().into_array(2)?;
                let input = input.as_any().downcast_ref::<Int32Array>().unwrap();
                let output = Arc::new(Int32Array::from_iter(
                    input.iter().map(|value| value.map(|value| value + 1)),
                )) as ArrayRef;
                arrays.push(Arc::downgrade(&output));
                Ok(ColumnarValue::Array(output))
            }),
        );
        let changes = [("a", "value"), ("b", "a"), ("c", "b")]
            .into_iter()
            .map(|(name, input)| {
                ColumnEvolution::add_column(
                    name,
                    DataType::Int32,
                    expression.call(vec![col(input)]),
                )
            })
            .collect();
        let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1, 2])))]);
        let mut layer = SchemaEvolutionLayer::try_new(changes).unwrap();
        if known_schema {
            layer = layer.with_source_schema(source.schema.clone());
        }
        let mut reader = source
            .open_layer(layer, Some(vec!["c".into()]))
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(
            output.column(0).to_data(),
            Int32Array::from(vec![4, 5]).to_data()
        );
        let arrays = arrays.lock().unwrap();
        assert_eq!(arrays.len(), 3);
        assert!(arrays[..2].iter().all(|array| array.strong_count() == 0));
        assert!(arrays[2].upgrade().is_some());
    }
}

#[tokio::test]
async fn sql_and_inferred_exprs_share_ordered_schema_changes_and_metadata() {
    let input = batch(Arc::new(Int32Array::from(vec![1, 2])));
    for known_schema in [false, true] {
        let changes = vec![
            ColumnEvolution::rename_column("value", "n"),
            ColumnEvolution::add_column_sql("next", "n + 1"),
            ColumnEvolution::change_data_type("next", DataType::Int64),
            ColumnEvolution::add_column_expr("alias", col("next")),
            ColumnEvolution::remove_column("n"),
            ColumnEvolution::add_column_sql("final", "alias + 1"),
            ColumnEvolution::add_column_sql("n", "final * 2"),
            ColumnEvolution::rename_column("final", "result"),
        ];
        let mut evolution = SchemaEvolutionLayer::try_new(changes).unwrap();
        if known_schema {
            evolution = evolution.with_source_schema(input.schema());
        }
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .open_layer(
                evolution,
                Some(vec!["result".into(), "n".into(), "alias".into()]),
            )
            .await
            .unwrap();
        let schema = &reader.metadata().schema;
        assert!(!schema.field(2).is_nullable());
        assert_eq!(
            schema.field(2).metadata()["open_arrow_access:generated_expression"],
            "n + 1"
        );
        let output = reader.next().await.unwrap().unwrap();
        for (column, expected) in [vec![3, 4], vec![6, 8], vec![2, 3]].iter().enumerate() {
            assert_eq!(
                output.column(column).to_data(),
                Int64Array::from(expected.clone()).to_data()
            );
        }
    }
}
