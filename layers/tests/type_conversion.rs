use std::{
    error::Error as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use futures::stream;

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    cast::AsArray, types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, Error, ErrorKind, ObjectRef, ReadOptions, ReadRequest,
    RecordMetadata, RecordReader, Result, async_trait, record_stream_reader,
};
use open_arrow_access_layers::{ColumnTypeSpec, TypeConversionLayer};
use open_arrow_access_test_support::{format, record};

fn layer(name: &str, data_type: DataType) -> TypeConversionLayer {
    TypeConversionLayer::try_new(vec![ColumnTypeSpec {
        column: name.into(),
        data_type,
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
    fail: bool,
    calls: Arc<AtomicUsize>,
}

impl MemoryAccessor {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            fail: false,
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

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request
            .options
            .resolve_projection(&self.batches[0].schema())?;
        let mut batches = self.batches.clone().into_iter().map(Ok).chain(
            self.fail
                .then(|| Err(Error::new(ErrorKind::Io, "read", "injected"))),
        );
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
async fn casts_timestamp_with_named_timezone() {
    let input = batch(Arc::new(StringArray::from(vec![
        Some("1970-01-01T00:00:01Z"),
        None,
    ])));
    let mut reader = MemoryAccessor::new(vec![input])
        .open_cast(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    let expected =
        TimestampMicrosecondArray::from(vec![Some(1_000_000), None]).with_timezone("UTC");
    assert_eq!(output.column(0).to_data(), expected.to_data());
}

#[tokio::test]
async fn invalid_values_and_upstream_errors_terminate_reading() {
    let input = batch(Arc::new(StringArray::from(vec!["42", "bad"])));
    let source = MemoryAccessor::new(vec![input]);
    let calls = source.calls.clone();
    let mut reader = source.open_cast(DataType::Int32).await.unwrap();
    let error = format::assert_terminal_error(reader.as_mut(), &calls, ErrorKind::Decode).await;
    assert!(error.to_string().contains("value"));
    assert!(error.source().is_some());
    let mut source = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["1"])))]);
    source.fail = true;
    let calls = source.calls.clone();
    let mut reader = source.open_cast(DataType::Int32).await.unwrap();
    assert!(reader.next().await.unwrap().is_some());
    format::assert_terminal_error(reader.as_mut(), &calls, ErrorKind::Io).await;
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
async fn invalid_declarations_fail_before_reading_and_extensions_are_protected() {
    for names in [vec![""], vec!["value", "value"]] {
        let specs = names
            .into_iter()
            .map(|name| ColumnTypeSpec {
                column: name.into(),
                data_type: DataType::Int32,
            })
            .collect();
        assert!(TypeConversionLayer::try_new(specs).is_err());
    }
    let input = batch(Arc::new(StringArray::from(vec!["1"])));
    for (name, target) in [
        ("missing", DataType::Int32),
        (
            "value",
            DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
        ),
    ] {
        let source = MemoryAccessor::new(vec![input.clone()]);
        let calls = source.calls.clone();
        assert!(
            source
                .layer(layer(name, target))
                .open(ReadRequest::new("test"))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false)
            .with_metadata([("ARROW:extension:name".into(), "custom".into())].into()),
    ]));
    let input = RecordBatch::try_new(schema, input.columns().to_vec()).unwrap();
    assert!(
        MemoryAccessor::new(vec![input])
            .open_cast(DataType::Int32)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn no_ops_share_batches_and_layers_compose_in_order() {
    let input = batch(Arc::new(StringArray::from(vec!["42"])));
    for conversion in [
        TypeConversionLayer::default(),
        layer("value", DataType::Utf8),
    ] {
        let mut reader = MemoryAccessor::new(vec![input.clone()])
            .layer(conversion)
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert!(Arc::ptr_eq(output.schema_ref(), input.schema_ref()));
        assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    }
    let mut reader = MemoryAccessor::new(vec![input])
        .layer(layer("value", DataType::Int64))
        .layer(layer("value", DataType::Int32))
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.column(0).as_primitive::<Int32Type>().value(0), 42);
}

#[test]
fn malformed_target_types_are_rejected_recursively() {
    for target in [
        DataType::FixedSizeBinary(-1),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), -1),
        DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32)),
        DataType::Time32(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Second),
        DataType::Map(
            Arc::new(Field::new("entries", DataType::Int32, false)),
            false,
        ),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, true),
                        Field::new("value", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        DataType::Union(UnionFields::empty(), UnionMode::Dense),
        DataType::Union(
            [(-1, Arc::new(Field::new("item", DataType::Int32, true)))]
                .into_iter()
                .collect(),
            UnionMode::Sparse,
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
        DataType::Decimal128(39, 0),
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(-1),
            true,
        ))),
    ] {
        let result = TypeConversionLayer::try_new(vec![ColumnTypeSpec {
            column: "value".into(),
            data_type: target,
        }]);
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn parquet_conversion_preserves_projection_and_batch_order() {
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    use open_arrow_access_test_support::MemoryAccessor as ByteAccessor;
    use std::num::NonZeroUsize;

    let bytes =
        open_arrow_access_test_support::fixture!("parquet", "ordered-uncompressed.parquet").await;
    let source = ParquetAccessor::new(ByteAccessor::new(&bytes, 1024).with_ranges());
    let reader = source
        .layer(layer("id", DataType::Int32))
        .open(ReadRequest::new("test.parquet").with_options(ReadOptions {
            projection: Some(vec!["title".into(), "id".into()]),
            batch_size: NonZeroUsize::new(2).unwrap(),
        }))
        .await
        .unwrap();
    let schema = reader.metadata().schema.clone();
    assert_eq!(schema.field(0).name(), "title");
    assert_eq!(schema.field(1).data_type(), &DataType::Int32);
    assert_eq!(
        record::collect_column::<Int32Type>(reader, 1).await,
        (0..7).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn decimal_rescaling_rejects_unsafe_pairs_before_reading() {
    use arrow_array::new_empty_array;

    let from = DataType::Decimal256(76, 0);
    let to = DataType::Decimal256(76, 60);
    let map = |data_type| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", data_type, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    };
    let pairs = [
        (from.clone(), to.clone()),
        (from.clone(), DataType::Decimal256(76, 52)),
        (
            union(vec![from.clone(), to.clone()]),
            dictionary(to.clone()),
        ),
        (DataType::Decimal128(38, 38), DataType::Decimal128(38, -128)),
        (DataType::Decimal128(38, -128), DataType::Decimal256(76, 60)),
        (
            DataType::List(item(from.clone())),
            DataType::List(item(to.clone())),
        ),
        (
            DataType::Struct(vec![item(from.clone())].into()),
            DataType::Struct(vec![item(to.clone())].into()),
        ),
        (dictionary(from.clone()), dictionary(to.clone())),
        (
            DataType::RunEndEncoded(
                Arc::new(Field::new("ends", DataType::Int32, false)),
                item(from.clone()),
            ),
            to.clone(),
        ),
        (union(vec![from.clone()]), to.clone()),
        (map(from.clone()), map(to.clone())),
        (DataType::FixedSizeList(item(from.clone()), 1), to.clone()),
        (from, DataType::List(item(to))),
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

    let overflow = Decimal256Array::from_iter_values([
        "1000000000000000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
    ])
    .with_precision_and_scale(76, 0)
    .unwrap();
    let mut reader = MemoryAccessor::new(vec![batch(Arc::new(overflow))])
        .open_cast(DataType::Decimal256(76, 1))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
}
