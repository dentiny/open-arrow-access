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
use open_arrow_access_layers::{FieldRemap, SchemaRemapLayer};

fn layer(name: &str, data_type: DataType) -> SchemaRemapLayer {
    SchemaRemapLayer::try_new(vec![FieldRemap {
        source_name: name.into(),
        target_name: None,
        target_data_type: Some(data_type),
    }])
    .unwrap()
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
    for conversion in [SchemaRemapLayer::default(), layer("value", DataType::Utf8)] {
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .layer(conversion)
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
async fn schema_remap_renames_casts_and_preserves_unmapped_fields() {
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
        let remap = SchemaRemapLayer::try_new(vec![
            FieldRemap {
                source_name: "pdf_url".into(),
                target_name: Some("pdf".into()),
                target_data_type: None,
            },
            FieldRemap {
                source_name: "count".into(),
                target_name: None,
                target_data_type: Some(DataType::Int32),
            },
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
async fn schema_remap_rejects_collisions_but_allows_simultaneous_swaps() {
    let input = RecordBatch::try_from_iter([
        ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let rename = |source: &str, target: &str| FieldRemap {
        source_name: source.into(),
        target_name: Some(target.into()),
        target_data_type: None,
    };
    let error = MemoryAccessor::new(vec![input.clone()])
        .layer(SchemaRemapLayer::try_new(vec![rename("a", "b")]).unwrap())
        .open(ReadRequest::new("test").with_options(ReadOptions {
            projection: Some(vec!["b".into()]),
            ..Default::default()
        }))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let mut reader = MemoryAccessor::new(vec![input.clone()])
        .layer(SchemaRemapLayer::try_new(vec![rename("a", "b"), rename("b", "a")]).unwrap())
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.schema().field(0).name(), "b");
    assert_eq!(output.schema().field(1).name(), "a");
    assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    assert!(Arc::ptr_eq(output.column(1), input.column(1)));
}
