use std::{
    collections::VecDeque,
    error::Error as _,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{
    ArrayRef, Int32Array, Int64Array, LargeBinaryArray, ListArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, Error, ErrorKind, ObjectRef, ReadOptions, ReadRequest,
    Reader, RecordMetadata, RecordReader, Result, async_trait,
};
use open_arrow_access_layers::{ColumnTypeSpec, TypeConversionLayer};

fn layer(name: &str, data_type: DataType) -> TypeConversionLayer {
    TypeConversionLayer::try_new(vec![ColumnTypeSpec {
        column: name.into(),
        data_type,
    }])
    .unwrap()
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
        let schema = self.batches[0].schema();
        let indices = request
            .options
            .projection
            .as_ref()
            .map(|names| {
                names
                    .iter()
                    .map(|name| schema.index_of(name))
                    .collect::<std::result::Result<Vec<_>, _>>()
            })
            .transpose()
            .map_err(|e| Error::new(ErrorKind::InvalidInput, "project", e.to_string()))?;
        let mut batches = self.batches.clone();
        let schema = if let Some(indices) = indices {
            batches = batches
                .iter()
                .map(|b| b.project(&indices).unwrap())
                .collect();
            Arc::new(schema.project(&indices).unwrap())
        } else {
            schema
        };
        Ok(Box::new(MemoryReader {
            batches: batches.into(),
            fail: self.fail,
            calls: self.calls.clone(),
            metadata: RecordMetadata {
                schema,
                source: Some(request.object),
            },
        }))
    }
}

struct MemoryReader {
    batches: VecDeque<RecordBatch>,
    metadata: RecordMetadata,
    calls: Arc<AtomicUsize>,
    fail: bool,
}

#[async_trait]
impl Reader for MemoryReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(batch) = self.batches.pop_front() {
            return Ok(Some(batch));
        }
        if std::mem::take(&mut self.fail) {
            return Err(Error::new(ErrorKind::Io, "read", "injected"));
        }
        Ok(None)
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
        .layer(layer("value", DataType::Int32))
        .open(ReadRequest::new("test"))
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
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap(),
                &Int32Array::from(vec![Some(42), None, Some(-7)])
            );
            assert!(Arc::ptr_eq(output.column(1), input.column(1)));
        }
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn binary_timestamp_and_nested_casts_use_arrow_semantics() {
    let cases: Vec<(ArrayRef, DataType, ArrayRef)> = vec![
        (
            Arc::new(StringArray::from(vec![Some("hello"), None])),
            DataType::LargeBinary,
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"hello".as_slice()),
                None,
            ])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("1970-01-01T00:00:01Z"), None])),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_000_000), None]).with_timezone("UTC"),
            ),
        ),
        (
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None]),
                None,
            ])),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            Arc::new(ListArray::from_iter_primitive::<
                arrow_array::types::Int64Type,
                _,
                _,
            >(vec![Some(vec![Some(1), None]), None])),
        ),
    ];
    for (input, target, expected) in cases {
        let mut reader = MemoryAccessor::new(vec![batch(input)])
            .layer(layer("value", target))
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.column(0).to_data(), expected.to_data());
    }
}

#[tokio::test]
async fn invalid_values_and_upstream_errors_terminate_reading() {
    for input in [
        Arc::new(StringArray::from(vec!["42", "bad"])) as ArrayRef,
        Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1])),
    ] {
        let source = MemoryAccessor::new(vec![batch(input)]);
        let calls = source.calls.clone();
        let mut reader = source
            .layer(layer("value", DataType::Int32))
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains("value"));
        assert!(error.source().is_some());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    let mut source = MemoryAccessor::new(vec![batch(Arc::new(StringArray::from(vec!["1"])))]);
    source.fail = true;
    let calls = source.calls.clone();
    let mut reader = source
        .layer(layer("value", DataType::Int32))
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Io);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
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
    for specs in [
        vec![ColumnTypeSpec {
            column: "".into(),
            data_type: DataType::Int32,
        }],
        vec![
            ColumnTypeSpec {
                column: "value".into(),
                data_type: DataType::Int32
            };
            2
        ],
    ] {
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
            .layer(layer("value", DataType::Int32))
            .open(ReadRequest::new("test"))
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
        assert!(Arc::ptr_eq(&reader.metadata().schema, input.schema_ref()));
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
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[test]
fn malformed_target_types_are_rejected_recursively() {
    use arrow_schema::{UnionFields, UnionMode};
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
        DataType::Decimal128(0, 0),
        DataType::Decimal128(39, 0),
        DataType::Decimal128(3, 4),
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
    let mut reader = source
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
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        ids.extend_from_slice(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(ids, (0..7).collect::<Vec<_>>());
}
