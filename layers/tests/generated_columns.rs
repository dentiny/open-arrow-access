use std::{
    collections::HashMap,
    error::Error as _,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, ErrorKind, ReadOptions, ReadRequest, RecordMetadata,
    RecordReader, Result, async_trait, record_stream_reader,
};
use open_arrow_access_layers::{
    ColumnTypeSpec, GeneratedColumnSpec, GeneratedColumnsLayer, TypeConversionLayer,
};
use open_arrow_access_test_support::{
    DropProbe, fixture,
    reader::{assert_error_contract, assert_finished, assert_terminal_error},
};

fn layer(specs: &[(&str, &str)]) -> GeneratedColumnsLayer {
    GeneratedColumnsLayer::try_new(
        specs
            .iter()
            .map(|(column, expression)| GeneratedColumnSpec {
                column: (*column).into(),
                expression: (*expression).into(),
            })
            .collect(),
    )
    .unwrap()
}

struct MemoryAccessor {
    batches: Vec<RecordBatch>,
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl MemoryAccessor {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            polls: Arc::default(),
            dropped: Arc::default(),
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
            read_parallel: false,
            read_with_range: false,
            list: false,
        }
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request
            .options
            .resolve_projection(&self.batches[0].schema())?;
        let mut batches = self.batches.clone().into_iter().map(Ok);
        let polls = self.polls.clone();
        let probe = DropProbe::new(self.dropped.clone());
        let stream = stream::poll_fn(move |_| {
            let _ = &probe;
            polls.fetch_add(1, Ordering::SeqCst);
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

fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("Count", DataType::Int64, true)
                    .with_metadata(HashMap::from([("unit".into(), "items".into())])),
                Field::new("text", DataType::Utf8, true).with_metadata(HashMap::from([(
                    "ARROW:extension:name".into(),
                    "example.text".into(),
                )])),
            ],
            HashMap::from([("dataset".into(), "test".into())]),
        )),
        vec![
            Arc::new(Int64Array::from(vec![Some(2), None, Some(4)])),
            Arc::new(StringArray::from(vec![Some("中文"), None, Some("abc")])),
        ],
    )
    .unwrap()
}

fn request(names: &[&str]) -> ReadRequest {
    ReadRequest::new("test").with_options(ReadOptions {
        projection: Some(names.iter().map(|name| (*name).into()).collect()),
        ..ReadOptions::default()
    })
}

#[tokio::test]
async fn expressions_preserve_order_metadata_nulls_and_source_buffers() {
    let input = input();
    let source = MemoryAccessor::new(vec![input.clone(), input.slice(0, 0), input.clone()]);
    let polls = source.polls.clone();
    let dropped = source.dropped.clone();
    let mut reader = source
        .layer(layer(&[
            ("length", "character_length(text)"),
            ("next", "coalesce(\"Count\", 0) + 1"),
            ("twice", "next * 2"),
            ("constant", "7"),
            ("started", "now()"),
            ("alias", "\"Count\""),
            ("nothing", "NULL"),
            ("text_alias", "text"),
            ("also_started", "now()"),
        ]))
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let schema = reader.metadata().schema.clone();
    assert_eq!(schema.metadata(), input.schema().metadata());
    assert_eq!(schema.field(0), input.schema().field(0));
    assert_eq!(
        schema.field(2).metadata()["open_arrow_access:generated_expression"],
        "character_length(text)"
    );
    assert_eq!(schema.field(7).metadata()["unit"], "items");
    assert_eq!(
        schema.field(9).metadata()["ARROW:extension:name"],
        "example.text"
    );
    assert_eq!(schema.field(3).data_type(), &DataType::Int64);
    assert!(!schema.field(3).is_nullable());
    assert_eq!(reader.metadata().source.as_ref().unwrap().locator(), "test");
    let mut started = None;
    for rows in [3, 0, 3] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), rows);
        assert_eq!(batch.column(8).logical_null_count(), rows);
        assert!(Arc::ptr_eq(batch.schema_ref(), &schema));
        if rows > 0 {
            assert_eq!(batch.column(6).to_data(), batch.column(10).to_data());
            assert!(Arc::ptr_eq(batch.column(0), input.column(0)));
            assert!(Arc::ptr_eq(batch.column(1), input.column(1)));
            assert!(Arc::ptr_eq(batch.column(7), input.column(0)));
            assert!(Arc::ptr_eq(batch.column(9), input.column(1)));
            assert_eq!(
                batch.column(2).to_data(),
                Int32Array::from(vec![Some(2), None, Some(3)]).to_data()
            );
            assert_eq!(
                batch.column(4).to_data(),
                Int64Array::from(vec![6, 2, 10]).to_data()
            );
            assert_eq!(
                batch.column(5).to_data(),
                Int64Array::from(vec![7; 3]).to_data()
            );
            if let Some(started) = &started {
                assert_eq!(&batch.column(6).to_data(), started);
            }
            started = Some(batch.column(6).to_data());
        }
    }
    assert!(reader.next().await.unwrap().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    assert_finished(reader.as_mut(), &polls, |m| {
        (m.schema.clone(), m.source.clone())
    })
    .await;
}

#[tokio::test]
async fn final_projection_keeps_dependencies_and_zero_column_row_counts() {
    for names in [vec!["twice"], vec!["twice", "text"], vec![]] {
        let mut reader = MemoryAccessor::new(vec![input()])
            .layer(layer(&[
                ("first", "coalesce(\"Count\", 0) + 1"),
                ("twice", "first * 2"),
            ]))
            .open(request(&names))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), names.len());
        assert_eq!(batch.schema(), reader.metadata().schema);
        if !names.is_empty() {
            assert_eq!(
                batch.column(0).to_data(),
                Int64Array::from(vec![6, 2, 10]).to_data()
            );
        }
    }
    // Constant broadcasts must also work when there are no input arrays.
    for rows in [0, 3] {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap();
        let mut reader = MemoryAccessor::new(vec![batch])
            .layer(layer(&[("x", "1"), ("y", "x + 1")]))
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), rows);
        assert_eq!(
            batch.column(1).to_data(),
            Int64Array::from(vec![2; rows]).to_data()
        );
    }
}

#[tokio::test]
async fn invalid_expressions_fail_at_open_even_when_hidden() {
    for specs in [
        vec![("Count", "1")], // Collision with hidden physical field.
        vec![("x", "unknown + 1")],
        vec![("x", "y + 1"), ("y", "2")],
        vec![("x", "x + 1")],
        vec![("x", "count + 1")], // Mixed case needs quotes.
        vec![("x", "1 +")],
        vec![("x", "1; SELECT 2")],
        vec![("x", "sum(\"Count\")")],
        vec![("x", "row_number() OVER ()")],
        vec![("x", "(SELECT 1)")],
        vec![("x", "random()")],
        vec![("x", "1 + random()")],
        vec![("x", "unnest(ARRAY[1, 2])")],
        vec![("x", "CASE WHEN false THEN sum(\"Count\") ELSE 0 END")],
        vec![("x", "CASE WHEN false THEN random() ELSE 0 END")],
    ] {
        let source = MemoryAccessor::new(vec![input()]);
        let polls = source.polls.clone();
        let error = source
            .layer(layer(&specs))
            .open(request(&["text"]))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{specs:?}: {error}");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    for names in [vec!["missing"], vec!["x", "x"]] {
        assert!(
            MemoryAccessor::new(vec![input()])
                .layer(layer(&[("x", "1")]))
                .open(request(&names))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn runtime_errors_release_input_even_when_the_generated_column_is_hidden() {
    // The first batch succeeds; the next divides by zero. A later batch must
    // never be polled, even if the caller keeps the failed reader alive.
    let input = input();
    let source = MemoryAccessor::new(vec![input.slice(0, 2), input.slice(2, 1), input]);
    let polls = source.polls.clone();
    let dropped = source.dropped.clone();
    let mut reader = source
        .layer(layer(&[("quotient", "12 / (\"Count\" - 4)")]))
        .open(request(&["text"]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(!dropped.load(Ordering::SeqCst));
    let error = assert_terminal_error(reader.as_mut(), &polls, ErrorKind::Decode, |m| {
        (m.schema.clone(), m.source.clone())
    })
    .await;
    assert_eq!(error.operation(), "generate_column");
    assert!(error.to_string().contains("quotient"));
    assert!(
        error
            .source()
            .unwrap()
            .is::<datafusion::error::DataFusionError>()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn composes_after_type_conversion() {
    let mut reader = MemoryAccessor::new(vec![input()])
        .layer(
            TypeConversionLayer::try_new(vec![ColumnTypeSpec {
                column: "Count".into(),
                data_type: DataType::Float64,
            }])
            .unwrap(),
        )
        .layer(layer(&[("x", "\"Count\" * 2")]))
        .open(request(&["x"]))
        .await
        .unwrap();
    assert_eq!(
        reader.metadata().schema.field(0).data_type(),
        &DataType::Float64
    );
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 3);
}

#[tokio::test]
async fn csv_and_parallel_reads_share_one_generation_context() {
    use open_arrow_access_file_format::csv::CsvAccessor;
    use open_arrow_access_layers::ParallelReadLayer;
    use open_arrow_access_test_support::MemoryAccessor as BytesAccessor;
    use std::num::NonZeroUsize;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, true),
    ]));
    let bytes = fixture!("csv", "good_no_header.csv").await;
    let source = CsvAccessor::new(BytesAccessor::new(&bytes, 1), schema.clone());
    let source = source
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .layer(layer(&[("x", "id * 2"), ("started", "now()")]));
    assert!(source.capability().read_parallel);
    let mut req = request(&["x", "started"]);
    req.options.batch_size = NonZeroUsize::MIN;
    let mut reader = source.open(req).await.unwrap();
    assert!(reader.capability().read_parallel);
    assert!(!reader.capability().list);
    assert!(!reader.capability().read_with_range);
    let mut started = None;
    for expected in [2, 4, 6] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch.column(0).to_data(),
            Int64Array::from(vec![expected]).to_data()
        );
        if let Some(started) = &started {
            assert_eq!(&batch.column(1).to_data(), started);
        }
        started = Some(batch.column(1).to_data());
    }
    assert!(reader.next().await.unwrap().is_none());
    assert_error_contract(
        |input| CsvAccessor::new(input, schema.clone()).layer(layer(&[("x", "id * 2")])),
        &bytes,
        request(&["x"]),
        &[0, bytes.len() - 1],
        |m| (m.schema.clone(), m.source.clone()),
        |_, _| {},
    )
    .await;
}

#[tokio::test]
async fn casts_preserve_values_and_nulls() {
    for expression in [
        r#"CAST("Count" AS INT)"#,
        r#"TRY_CAST("Count" AS INT)"#,
        r#"arrow_cast("Count", 'Int32')"#,
        r#"arrow_try_cast("Count", 'Int32')"#,
        r#"cast_to_type("Count", CAST(NULL AS INT))"#,
        r#"try_cast_to_type("Count", CAST(NULL AS INT))"#,
        r#"arrow_cast(cast_to_type("Count", CAST(NULL AS INT)), 'Int32')"#,
    ] {
        let mut reader = MemoryAccessor::new(vec![input()])
            .layer(layer(&[("x", expression)]))
            .open(request(&["x"]))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(
            batch.column(0).to_data(),
            arrow_array::Int32Array::from(vec![Some(2), None, Some(4)]).to_data(),
            "{expression}"
        );
    }
}
