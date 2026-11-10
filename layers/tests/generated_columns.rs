use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, ErrorKind, ReadOptions, ReadRequest, RecordMetadata,
    RecordReader, Result, async_trait, record_stream_reader,
};
use open_arrow_access_layers::{
    ColumnTypeSpec, GeneratedColumnSpec, GeneratedColumnsLayer, TypeConversionLayer,
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
    drops: Arc<AtomicUsize>,
}

struct DropGuard(Arc<AtomicUsize>);
impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl MemoryAccessor {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            polls: Arc::default(),
            drops: Arc::default(),
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
        let guard = DropGuard(self.drops.clone());
        let stream = stream::poll_fn(move |_| {
            let _ = &guard;
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
    let polls_at_eof = polls.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), polls_at_eof);
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
        vec![(
            "x",
            r#"arrow_cast(NULL, 'RunEndEncoded("run_ends": non-null Int16, "values": Int64)')"#,
        )],
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
async fn runtime_failure_is_terminal_and_releases_input() {
    let good =
        RecordBatch::try_from_iter([("text", Arc::new(StringArray::from(vec!["42"])) as ArrayRef)])
            .unwrap();
    let bad = RecordBatch::try_from_iter([(
        "text",
        Arc::new(StringArray::from(vec!["bad"])) as ArrayRef,
    )])
    .unwrap();
    let source = MemoryAccessor::new(vec![good.clone(), bad, good]);
    let polls = source.polls.clone();
    let drops = source.drops.clone();
    let mut reader = source
        .layer(layer(&[("value", "CAST(text AS BIGINT)")]))
        .open(request(&[]))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("value"));
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn empty_layer_passes_through_and_drop_stops_reading() {
    let input = input();
    for generation in [GeneratedColumnsLayer::default(), layer(&[("x", "1")])] {
        let source = MemoryAccessor::new(vec![input.clone()]);
        let polls = source.polls.clone();
        let drops = source.drops.clone();
        let mut reader = source
            .layer(generation)
            .open(ReadRequest::new("test"))
            .await
            .unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert!(Arc::ptr_eq(batch.column(0), input.column(0)));
        drop(reader);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
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

#[test]
fn rejects_invalid_declarations_without_io() {
    for specs in [
        vec![("", "1")],
        vec![(" ", "1")],
        vec![("x", " ")],
        vec![("x", "1"), ("x", "2")],
    ] {
        assert!(
            GeneratedColumnsLayer::try_new(
                specs
                    .into_iter()
                    .map(|(column, expression)| GeneratedColumnSpec {
                        column: column.into(),
                        expression: expression.into()
                    })
                    .collect()
            )
            .is_err()
        );
    }
}

// Reuses the same source and assertions; opt in to timing on an otherwise idle
// machine. Keep open/SQL planning separate from steady batch evaluation.
#[tokio::test]
#[ignore = "manual release performance check"]
async fn generated_columns_throughput() {
    use std::{hint::black_box, time::Instant};
    let batch = RecordBatch::try_from_iter([
        (
            "n",
            Arc::new(Int64Array::from_iter_values(0..8192)) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["generated columns 中文"; 8192])) as ArrayRef,
        ),
    ])
    .unwrap();
    let batches = vec![batch; 1024];
    for (name, specs) in [
        ("direct", None),
        ("empty", Some(vec![])),
        ("arithmetic", Some(vec![("x", "n * 2")])),
        (
            "string+dependency",
            Some(vec![
                ("length", "character_length(text)"),
                ("twice", "length * 2"),
            ]),
        ),
    ] {
        let mut timings = Vec::new();
        for _ in 0..5 {
            let source = MemoryAccessor::new(batches.clone());
            let start = Instant::now();
            let mut reader = match &specs {
                None => source.open(ReadRequest::new("perf")).await.unwrap(),
                Some(specs) => source
                    .layer(layer(specs))
                    .open(ReadRequest::new("perf"))
                    .await
                    .unwrap(),
            };
            let open = start.elapsed();
            let start = Instant::now();
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                rows += batch.num_rows();
                black_box(batch);
            }
            assert_eq!(rows, 8192 * 1024);
            timings.push((start.elapsed(), open));
        }
        timings.sort();
        let (scan, open) = timings[2];
        println!(
            "{name}: median scan {scan:?}, open {open:?}, {:.1} M rows/s",
            (8192.0 * 1024.0) / scan.as_secs_f64() / 1e6
        );
    }
}

#[tokio::test]
async fn csv_and_parallel_reads_share_one_generation_context() {
    use open_arrow_access_file_format::csv::CsvAccessor;
    use open_arrow_access_layers::ParallelReadLayer;
    use open_arrow_access_test_support::MemoryAccessor as BytesAccessor;
    use std::num::NonZeroUsize;

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let source = CsvAccessor::new(BytesAccessor::new(b"1\n2\n3\n4\n", 1), schema);
    let source = source
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .layer(layer(&[("x", "n * 2"), ("started", "now()")]));
    assert!(source.capability().read_parallel);
    let mut req = request(&["x", "started"]);
    req.options.batch_size = NonZeroUsize::MIN;
    let mut reader = source.open(req).await.unwrap();
    assert!(reader.capability().read_parallel);
    assert!(!reader.capability().list);
    assert!(!reader.capability().read_with_range);
    let mut started = None;
    for expected in [2, 4, 6, 8] {
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
}

#[tokio::test]
async fn unsafe_decimal_rescaling_fails_before_evaluation() {
    use arrow_array::Decimal256Array;
    let batch = RecordBatch::try_from_iter([(
        "n",
        Arc::new(
            Decimal256Array::from_iter([Some(1.into())])
                .with_precision_and_scale(76, 0)
                .unwrap(),
        ) as ArrayRef,
    )])
    .unwrap();
    for expression in [
        "CAST(n AS DECIMAL(76, 60))",
        "TRY_CAST(n AS DECIMAL(76, 60))",
        "arrow_cast(n, 'Decimal256(76, 60)')",
        "arrow_try_cast(n, 'Decimal256(76, 60)')",
        "cast_to_type(n, arrow_cast(1, 'Decimal256(76, 60)'))",
        "try_cast_to_type(n, arrow_cast(1, 'Decimal256(76, 60)'))",
        "CAST(CAST(1 AS DECIMAL(76, 0)) AS DECIMAL(76, 60))",
    ] {
        let source = MemoryAccessor::new(vec![batch.clone()]);
        let polls = source.polls.clone();
        let error = source
            .layer(layer(&[("x", expression)]))
            .open(ReadRequest::new("test"))
            .await
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("unsafe Decimal rescale"),
            "{error}"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    let mut reader = MemoryAccessor::new(vec![batch])
        .layer(layer(&[("x", "CAST(n AS DECIMAL(76, 51))")]))
        .open(ReadRequest::new("test"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);

    let batch = RecordBatch::try_from_iter([(
        "n",
        Arc::new(
            arrow_array::Decimal128Array::from(vec![1])
                .with_precision_and_scale(38, -128)
                .unwrap(),
        ) as ArrayRef,
    )])
    .unwrap();
    assert!(
        MemoryAccessor::new(vec![batch])
            .layer(layer(&[("x", "CAST(n AS DECIMAL(38, 0))")]))
            .open(ReadRequest::new("test"))
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("unsafe Decimal rescale")
    );
}

#[tokio::test]
async fn incompatible_struct_cast_fails_at_open() {
    use arrow_array::StructArray;
    let field = Arc::new(Field::new("a", DataType::Int64, true));
    let array = StructArray::from(vec![(
        field,
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )]);
    let batch = RecordBatch::try_from_iter([("n", Arc::new(array) as ArrayRef)]).unwrap();
    let error = MemoryAccessor::new(vec![batch])
        .layer(layer(&[(
            "x",
            r#"arrow_cast(n, 'Struct("a": Int64, "b": Int64)')"#,
        )]))
        .open(ReadRequest::new("test"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("cannot cast"), "{error}");
}
