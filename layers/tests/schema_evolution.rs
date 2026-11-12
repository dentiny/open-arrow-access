use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use futures::stream;

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, cast::AsArray,
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, ErrorKind, ObjectRef, OrderRequirement, ReadOptions,
    ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result, async_trait,
    record_stream_reader,
};
use open_arrow_access_layers::{ColumnEvolution, SchemaEvolutionLayer};

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

struct MemoryAccessor {
    batches: Vec<RecordBatch>,
    calls: Arc<AtomicUsize>,
}

impl MemoryAccessor {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            calls: Arc::default(),
        }
    }

    async fn open_cast(self, target: DataType) -> Result<Box<dyn RecordReader>> {
        self.layer(layer("value", target))
            .open(ReadRequest::new("test"))
            .await
    }
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request
            .options
            .resolve_projection(&self.batches[0].schema())?;
        let mut batches = self.batches.clone().into_iter().map(Ok);
        let calls = self.calls.clone();
        let stream = stream::poll_fn(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(batches.next())
        });
        Ok(Box::new(record_stream_reader(
            stream,
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            projection,
            OrderRequirement::Reorderable,
        )))
    }
}

fn batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([("value", array)]).unwrap()
}

#[tokio::test]
async fn casts_preserve_metadata_nulls_order_and_unchanged_buffers() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("value", DataType::Utf8, true)
                .with_metadata([("unit".into(), "count".into())].into()),
            Field::new("keep", DataType::Int64, false),
        ],
        [("dataset".into(), "test".into())].into(),
    ));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("42"), None, Some("-7")])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let mut reader = MemoryAccessor::new(vec![input.clone(), input.slice(1, 0), input.clone()])
        .open_cast(DataType::Int32)
        .await
        .unwrap();
    let expected = reader.metadata().schema.clone();
    assert_eq!(expected.field(0).data_type(), &DataType::Int32);
    assert_eq!(expected.field(0).metadata(), schema.field(0).metadata());
    assert_eq!(expected.metadata(), schema.metadata());
    assert_eq!(reader.metadata().source, Some(ObjectRef::new("test")));
    assert!(reader.capability().read);
    for rows in [3, 0, 3] {
        let output = reader.next().await.unwrap().unwrap();
        assert!(Arc::ptr_eq(output.schema_ref(), &expected));
        assert_eq!(output.num_rows(), rows);
        if rows > 0 {
            assert_eq!(
                output.column(0).as_primitive::<Int32Type>(),
                &Int32Array::from(vec![Some(42), None, Some(-7)])
            );
            assert!(Arc::ptr_eq(output.column(1), input.column(1)));
        }
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn incompatible_types_fail_before_reading() {
    let input = batch(Arc::new(Int32Array::from(vec![42])));
    let source = MemoryAccessor::new(vec![input]);
    let calls = source.calls.clone();
    let target = DataType::Struct(vec![item(DataType::Int32)].into());
    let error = source.open_cast(target).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(error.to_string().contains("cannot cast Int32 to Struct"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
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
        let request = ReadRequest::new("test").with_options(ReadOptions {
            projection: Some(names.iter().map(|s| (*s).into()).collect()),
            ..ReadOptions::default()
        });
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .layer(layer("value", DataType::Int32))
            .open(request)
            .await
            .unwrap();
        assert_eq!(
            reader
                .metadata()
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            names
        );
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_columns(), names.len());
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.schema(), reader.metadata().schema);
    }
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["bad"])))])
        .layer(layer("value", DataType::Int32))
        .open(ReadRequest::new("test").with_options(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        }))
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
async fn decimal_rescaling_rejects_unsafe_pairs_before_reading() {
    use arrow_array::new_empty_array;

    let from = DataType::Decimal256(76, 0);
    let to = DataType::Decimal256(76, 60);
    let pairs = [
        (from.clone(), to.clone()),
        (from.clone(), DataType::Decimal256(76, 52)),
        (DataType::Decimal128(38, 38), DataType::Decimal128(38, -128)),
        (
            DataType::List(item(dictionary(from.clone()))),
            DataType::List(item(dictionary(to.clone()))),
        ),
        (union(vec![from, to.clone()]), dictionary(to)),
    ];
    for (from, to) in pairs {
        let source = MemoryAccessor::new(vec![batch(new_empty_array(&from))]);
        let calls = source.calls.clone();
        let error = source.open_cast(to).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(error.to_string().contains("Decimal rescale"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn decimal_guard_preserves_safe_rescaling_and_struct_field_mapping() {
    use arrow_array::{Decimal256Array, new_empty_array};

    let input = Decimal256Array::from_iter([Some(1.into()), None])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(input))])
        .open_cast(DataType::Decimal256(76, 51))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    let expected =
        Decimal256Array::from_iter([Some(format!("1{}", "0".repeat(51)).parse().unwrap()), None])
            .with_precision_and_scale(76, 51)
            .unwrap();
    assert_eq!(output.column(0).to_data(), expected.to_data());

    let fields = vec![
        Field::new("a", DataType::Decimal256(76, 0), true),
        Field::new("b", DataType::Decimal256(76, 60), true),
    ];
    let from = DataType::Struct(fields.clone().into());
    let to = DataType::Struct(fields.into_iter().rev().collect());
    let mut reader = MemoryAccessor::new(vec![batch(new_empty_array(&from))])
        .open_cast(to)
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());

    let union = union(vec![
        DataType::Decimal256(76, 0),
        DataType::Decimal256(76, 60),
    ]);
    let from = dictionary(dictionary(union));
    let to = dictionary(DataType::Decimal256(76, 60));
    let mut reader = MemoryAccessor::new(vec![batch(new_empty_array(&from))])
        .open_cast(to)
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
}

#[tokio::test]
async fn timestamp_casts_accept_minute_precision_and_preserve_arrow_formats() {
    use arrow_schema::TimeUnit;
    let cases = [
        ("2000-05-31T04:00Z", "2000-05-31T04:00:00Z"),
        ("2000-05-31 06:00+02:00", "2000-05-31T04:00:00Z"),
        ("2000-05-31t04:00z", "2000-05-31T04:00:00Z"),
        ("2000-05-31T04:00", "2000-05-31T04:00:00"),
        ("2000-05-31 04:00", "2000-05-31T04:00:00"),
        (
            "2000-05-31T04:00:01.123456789Z",
            "2000-05-31T04:00:01.123456789Z",
        ),
    ];
    for source_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for timezone in [None, Some("UTC".into()), Some("America/New_York".into())] {
                let target = DataType::Timestamp(unit, timezone);
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
                        .map(|(_, expected)| Some(*expected))
                        .chain([None])
                        .collect::<Vec<_>>(),
                );
                let input = arrow_cast::cast(&input, &source_type).unwrap();
                let expected = arrow_cast::cast(&expected, &target).unwrap();
                let mut reader = MemoryAccessor::new(vec![batch(input)])
                    .open_cast(target)
                    .await
                    .unwrap();
                assert_eq!(reader.next().await.unwrap().unwrap().column(0), &expected);
                assert!(reader.next().await.unwrap().is_none());
            }
        }
    }
}

#[tokio::test]
async fn timestamp_fallback_rejects_invalid_ambiguous_and_overflowing_values() {
    use arrow_schema::TimeUnit;
    for (value, unit, timezone) in [
        ("invalid", TimeUnit::Second, None),
        ("2000-02-30T04:00Z", TimeUnit::Second, None),
        (
            "2025-11-02T01:30",
            TimeUnit::Second,
            Some("America/New_York"),
        ),
        (
            "2025-03-09T02:30",
            TimeUnit::Second,
            Some("America/New_York"),
        ),
        ("2500-01-01T00:00Z", TimeUnit::Nanosecond, None),
    ] {
        let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec![value])))])
            .open_cast(DataType::Timestamp(unit, timezone.map(Into::into)))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode, "{value}");
        assert_eq!(error.operation(), "cast");
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn schema_evolution_renames_casts_and_preserves_unmapped_fields() {
    let input = RecordBatch::try_from_iter([
        (
            "count",
            Arc::new(StringArray::from(vec![Some("42"), None])) as ArrayRef,
        ),
        (
            "pdf_url",
            Arc::new(StringArray::from(vec!["https://example.test/a.pdf"; 2])) as ArrayRef,
        ),
        ("keep", Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef),
    ])
    .unwrap();
    for names in [vec!["pdf", "count", "keep"], vec![]] {
        let remap = SchemaEvolutionLayer::try_new(vec![
            ColumnEvolution::rename_column("pdf_url", "pdf"),
            ColumnEvolution::change_data_type("count", DataType::Int32),
        ])
        .unwrap();
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .layer(remap)
            .open(ReadRequest::new("test").with_options(ReadOptions {
                projection: Some(names.iter().map(|name| (*name).into()).collect()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.schema(), reader.metadata().schema);
        assert_eq!(
            output
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            names
        );
        if !names.is_empty() {
            assert!(Arc::ptr_eq(output.column(0), input.column(1)));
            assert_eq!(
                output.column(1).to_data(),
                Int32Array::from(vec![Some(42), None]).to_data()
            );
            assert!(Arc::ptr_eq(output.column(2), input.column(2)));
        }
    }
}

#[tokio::test]
async fn schema_evolution_rejects_collisions_but_allows_simultaneous_swaps() {
    let input = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let rename = |source: &str, target: &str| ColumnEvolution::rename_column(source, target);
    let error = MemoryAccessor::new(vec![input.clone()])
        .layer(SchemaEvolutionLayer::try_new(vec![rename("a", "b")]).unwrap())
        .open(ReadRequest::new("test").with_options(ReadOptions {
            projection: Some(vec!["b".into()]),
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let mut reader = MemoryAccessor::new(vec![input.clone()])
        .layer(SchemaEvolutionLayer::try_new(vec![rename("a", "b"), rename("b", "a")]).unwrap())
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.schema().field(0).name(), "b");
    assert_eq!(output.schema().field(1).name(), "a");
    assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    assert!(Arc::ptr_eq(output.column(1), input.column(1)));
}

#[tokio::test]
async fn evolution_combines_all_operations_and_functors_see_the_original_batch() {
    let source_schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("value", DataType::Utf8, true)
                .with_metadata([("unit".into(), "count".into())].into()),
            Field::new("keep", DataType::Int64, false),
            Field::new("remove", DataType::Int32, false),
        ],
        [("dataset".into(), "evolution".into())].into(),
    ));
    let input = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("42"), None])),
            Arc::new(Int64Array::from(vec![3, 4])),
            Arc::new(Int32Array::from(vec![5, 6])),
        ],
    )
    .unwrap();
    for projection in [
        None,
        Some(vec!["added".into(), "count".into()]),
        Some(vec![]),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let expected = source_schema.clone();
        let changes = vec![
            ColumnEvolution::rename_column("value", "count"),
            ColumnEvolution::change_data_type("value", DataType::Int32),
            ColumnEvolution::remove_column("remove"),
            ColumnEvolution::add_column("added", DataType::Int32, move |batch| {
                seen.fetch_add(1, Ordering::SeqCst);
                assert_eq!(batch.schema(), expected);
                assert!(batch.column_by_name("count").is_none());
                Ok(batch.column_by_name("remove").unwrap().clone())
            }),
        ];
        let mut reader = MemoryAccessor::new(vec![input.clone(), input.slice(0, 0)])
            .layer(SchemaEvolutionLayer::try_new(changes).unwrap())
            .open(ReadRequest::new("test").with_options(ReadOptions {
                projection: projection.clone(),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(reader.metadata().source, Some(ObjectRef::new("test")));
        assert_eq!(reader.capability().ordering, OrderRequirement::Reorderable);
        assert_eq!(
            reader.metadata().schema.metadata(),
            source_schema.metadata()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_rows(), 2);
        assert!(Arc::ptr_eq(output.schema_ref(), &reader.metadata().schema));
        let names: Vec<_> = output
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            projection.unwrap_or_else(|| vec!["count".into(), "keep".into(), "added".into()])
        );
        if let Some(column) = output.column_by_name("count") {
            assert_eq!(
                column.to_data(),
                Int32Array::from(vec![Some(42), None]).to_data()
            );
            assert_eq!(
                output.schema().field_with_name("count").unwrap().metadata(),
                source_schema.field(0).metadata()
            );
        }
        if let Some(column) = output.column_by_name("keep") {
            assert!(Arc::ptr_eq(column, input.column(1)));
        }
        if let Some(column) = output.column_by_name("added") {
            assert!(Arc::ptr_eq(column, input.column(2)));
        }
        let empty = reader.next().await.unwrap().unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), reader.metadata().schema);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn evolution_can_reuse_removed_and_renamed_source_names() {
    let input = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let mut reader = MemoryAccessor::new(vec![input.clone()])
        .layer(
            SchemaEvolutionLayer::try_new(vec![
                ColumnEvolution::remove_column("b"),
                ColumnEvolution::rename_column("a", "b"),
                ColumnEvolution::add_column("a", DataType::Int64, |batch| {
                    Ok(batch.column(1).clone())
                }),
            ])
            .unwrap(),
        )
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.schema().field(0).name(), "b");
    assert_eq!(output.schema().field(1).name(), "a");
    assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    assert!(Arc::ptr_eq(output.column(1), input.column(1)));
}

#[test]
fn evolution_rejects_ambiguous_declarations() {
    for changes in [
        vec![ColumnEvolution::remove_column("")],
        vec![ColumnEvolution::rename_column("a", "")],
        vec![
            ColumnEvolution::remove_column("a"),
            ColumnEvolution::remove_column("a"),
        ],
        vec![
            ColumnEvolution::remove_column("a"),
            ColumnEvolution::rename_column("a", "b"),
        ],
        vec![
            ColumnEvolution::change_data_type("a", DataType::Int32),
            ColumnEvolution::remove_column("a"),
        ],
        vec![
            ColumnEvolution::rename_column("a", "b"),
            ColumnEvolution::rename_column("a", "c"),
        ],
        vec![
            ColumnEvolution::change_data_type("a", DataType::Int32),
            ColumnEvolution::change_data_type("a", DataType::Int64),
        ],
        vec![
            ColumnEvolution::add_column("a", DataType::Int32, |b| Ok(b.column(0).clone())),
            ColumnEvolution::add_column("a", DataType::Int32, |b| Ok(b.column(0).clone())),
        ],
    ] {
        assert_eq!(
            SchemaEvolutionLayer::try_new(changes).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[tokio::test]
async fn evolution_rejects_unknown_columns_and_output_collisions_before_reading() {
    for changes in [
        vec![ColumnEvolution::remove_column("missing")],
        vec![ColumnEvolution::rename_column("missing", "renamed")],
        vec![ColumnEvolution::change_data_type(
            "missing",
            DataType::Int32,
        )],
        vec![ColumnEvolution::add_column("value", DataType::Int32, |b| {
            Ok(b.column(0).clone())
        })],
        vec![
            ColumnEvolution::rename_column("value", "added"),
            ColumnEvolution::add_column("added", DataType::Int32, |b| Ok(b.column(0).clone())),
        ],
    ] {
        let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1])))]);
        let calls = source.calls.clone();
        let error = source
            .layer(SchemaEvolutionLayer::try_new(changes).unwrap())
            .open(ReadRequest::new("test").with_options(ReadOptions {
                projection: Some(vec![]),
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn evolution_checks_functor_type_and_length_even_when_not_projected() {
    for (declared, returned) in [
        (
            DataType::Int64,
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1, 2])))]);
        let reads = source.calls.clone();
        let mut reader = source
            .layer(
                SchemaEvolutionLayer::try_new(vec![ColumnEvolution::add_column(
                    "added",
                    declared,
                    move |_| {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(returned.clone())
                    },
                )])
                .unwrap(),
            )
            .open(ReadRequest::new("test").with_options(ReadOptions {
                projection: Some(vec![]),
                ..Default::default()
            }))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "evolve_schema");
        assert!(error.to_string().contains("added"));
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn evolution_preserves_callback_errors_and_stops_reading() {
    let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1])))]);
    let calls = source.calls.clone();
    let mut reader = source
        .layer(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::add_column(
                "added",
                DataType::Int32,
                |_| {
                    Err(open_arrow_access_core::Error::new(
                        ErrorKind::Io,
                        "custom_functor",
                        "failed",
                    ))
                },
            )])
            .unwrap(),
        )
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    let cause = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<open_arrow_access_core::Error>()
        .unwrap();
    assert_eq!(cause.operation(), "custom_functor");
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn evolution_can_remove_all_columns_without_losing_rows() {
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1, 2, 3])))])
        .layer(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::remove_column("value")]).unwrap(),
        )
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 0);
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.num_columns(), 0);
    assert_eq!(output.num_rows(), 3);
}

#[tokio::test]
async fn evolution_appends_functors_in_order_and_reuses_them_across_readers() {
    let source = MemoryAccessor::new(vec![batch(Arc::new(Int32Array::from(vec![1, 2])))]);
    let calls = Arc::new(AtomicUsize::new(0));
    let first = calls.clone();
    let second = calls.clone();
    let changes = SchemaEvolutionLayer::try_new(vec![
        ColumnEvolution::add_column("first", DataType::Int32, move |batch| {
            assert_eq!(first.fetch_add(1, Ordering::SeqCst) % 2, 0);
            Ok(batch.column(0).clone())
        }),
        ColumnEvolution::add_column("second", DataType::Int32, move |batch| {
            assert_eq!(second.fetch_add(1, Ordering::SeqCst) % 2, 1);
            assert!(batch.column_by_name("first").is_none());
            Ok(batch.column(0).clone())
        }),
    ])
    .unwrap();
    let accessor = source.layer(changes.clone());
    for _ in 0..2 {
        let mut reader = accessor.open(ReadRequest::new("test")).await.unwrap();
        let output = reader.next().await.unwrap().unwrap();
        let names: Vec<_> = output
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, ["value", "first", "second"]);
        assert!(Arc::ptr_eq(output.column(0), output.column(1)));
        assert!(Arc::ptr_eq(output.column(0), output.column(2)));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn evolution_rename_and_cast_use_source_names_in_either_declaration_order() {
    for changes in [
        vec![
            ColumnEvolution::rename_column("value", "count"),
            ColumnEvolution::change_data_type("value", DataType::Int32),
        ],
        vec![
            ColumnEvolution::change_data_type("value", DataType::Int32),
            ColumnEvolution::rename_column("value", "count"),
        ],
    ] {
        let mut reader = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["12"])))])
            .layer(SchemaEvolutionLayer::try_new(changes).unwrap())
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        assert_eq!(reader.metadata().schema.field(0).name(), "count");
        assert_eq!(
            reader.metadata().schema.field(0).data_type(),
            &DataType::Int32
        );
        assert_eq!(
            reader.next().await.unwrap().unwrap().column(0).to_data(),
            Int32Array::from(vec![12]).to_data()
        );
    }
}
