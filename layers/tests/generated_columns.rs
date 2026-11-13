use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, Capability, ErrorKind, OrderRequirement, ReadOptions, ReadRequest,
    RecordCapability, RecordMetadata, RecordReader, Result, async_trait, record_stream_reader,
};
use open_arrow_access_layers::{
    ColumnEvolution, GeneratedColumnSpec, GeneratedColumnsLayer, SchemaEvolutionLayer,
};
use open_arrow_access_test_support::reader::assert_finished;

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
    projections: Arc<Mutex<Vec<Option<Vec<String>>>>>,
}

impl MemoryAccessor {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            polls: Arc::default(),
            projections: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_parallel: false,
            read_with_range: false,
            list: false,
        }
        .with_ordering(OrderRequirement::Reorderable)
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.projections
            .lock()
            .unwrap()
            .push(request.options.projection.clone());
        let (schema, projection) = request
            .options
            .resolve_projection(&self.batches[0].schema())?;
        let mut batches = self.batches.clone().into_iter().map(Ok);
        let polls = self.polls.clone();
        let stream = stream::poll_fn(move |_| {
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
            OrderRequirement::Reorderable,
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
async fn unused_generated_values_still_execute_and_fail_terminally() {
    let source = MemoryAccessor::new(vec![input(), input()]);
    let polls = source.polls.clone();
    let mut reader = source
        .layer(layer(&[("unused", "CAST(text AS BIGINT)"), ("x", "1")]))
        .open(request(&["x"]))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert!(error.to_string().contains("unused"), "{error}");
    assert_finished(reader.as_mut(), &polls, |m| {
        (m.schema.clone(), m.source.clone())
    })
    .await;
}

#[tokio::test]
async fn composes_after_schema_evolution() {
    let mut reader = MemoryAccessor::new(vec![input()])
        .layer(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::change_data_type(
                "Count",
                DataType::Float64,
            )])
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

#[tokio::test]
async fn projection_pushdown_reads_real_dependencies_and_remaps_column_indices() {
    let original = input();
    let mut fields = vec![Arc::new(Field::new("unused", DataType::Utf8, false))];
    fields.extend(original.schema().fields().iter().cloned());
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(StringArray::from(vec!["wide"; 3]))];
    columns.extend(original.columns().iter().cloned());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema = batch.schema();
    let source = MemoryAccessor::new(vec![batch]);
    let projections = source.projections.clone();
    let polls = source.polls.clone();
    let mut reader = source
        .layer(
            layer(&[
                ("next", "\"Count\" + 1 /* unused */"),
                ("twice", "next * 2"),
                ("literal", "'unused'"),
            ])
            .with_source_schema(schema),
        )
        .open(request(&["twice"]))
        .await
        .unwrap();
    assert_eq!(
        *projections.lock().unwrap(),
        vec![Some(vec!["Count".into()])]
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![Some(6), None, Some(10)])
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn compact_inputs_preserve_sql_names_nested_access_and_constants() {
    use arrow_array::{ArrayRef, StructArray};
    let nested = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("Count", DataType::Int64, false)),
        Arc::new(Int64Array::from(vec![4])) as ArrayRef,
    )]));
    let input = RecordBatch::try_from_iter([
        ("record", nested as ArrayRef),
        ("Count", Arc::new(Int64Array::from(vec![9])) as ArrayRef),
        (
            "unused",
            Arc::new(StringArray::from(vec!["payload"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let config = layer(&[
        ("nested", "record.\"Count\""),
        ("combined", "coalesce(nested, \"Count\") + 1 AS ignored"),
        ("constant", "'unused Count' /* record */"),
    ])
    .with_source_schema(input.schema());
    let source = MemoryAccessor::new(vec![input]);
    let projections = source.projections.clone();
    let mut reader = source
        .layer(config)
        .open(request(&["combined", "constant"]))
        .await
        .unwrap();
    assert_eq!(
        *projections.lock().unwrap(),
        vec![Some(vec!["record".into(), "Count".into()])]
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch.column(0).to_data(),
        Int64Array::from(vec![5]).to_data()
    );
    assert_eq!(
        batch.column(1).to_data(),
        StringArray::from(vec!["unused Count"]).to_data()
    );
    assert!(reader.next().await.unwrap().is_none());
}
