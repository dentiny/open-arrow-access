use std::sync::{Arc, atomic::Ordering};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, cast::AsArray, types::Int64Type,
};
use arrow_schema::DataType;
use datafusion::{
    logical_expr::{ColumnarValue, Expr, Volatility, create_udf},
    prelude::{col, lit},
};
use futures::TryStreamExt;
use more_asserts::assert_gt;
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, ReadRequest, into_stream};
use open_arrow_access_layers::FilterLayer;
use open_arrow_access_test_support::{
    MemoryRecordAccessor,
    record::{collect_column, collect_values},
};

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            "data",
            Arc::new(BinaryArray::from(vec![
                Some(b"".as_slice()),
                Some(b"\xff\x00\x80".as_slice()),
                Some(b"ab".as_slice()),
                None,
                Some(b"x".as_slice()),
                Some(b"long".as_slice()),
            ])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn multiple_predicates_filter_whole_rows_in_order() {
    let batch = batch();
    let memory = MemoryRecordAccessor::new(vec![
        batch.slice(0, 2),
        batch.slice(2, 2),
        batch.slice(4, 2),
    ]);
    let source = memory.clone().layer(FilterLayer::new(vec![
        size_predicate(3),
        col("id").gt(lit(0i64)),
    ]));
    let reader = source.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(memory.polls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.metadata().schema, batch.schema());
    assert_eq!(reader.capability(), memory.capability());
    let batches: Vec<_> = into_stream(reader).try_collect().await.unwrap();
    for output in &batches {
        assert_gt!(output.num_rows(), 0);
        assert_eq!(output.schema(), batch.schema());
    }
    assert_eq!(collect_values::<Int64Type>(&batches, 0), [1, 2, 4]);
}

// The caller owns the predicate logic and captured configuration. The layer
// receives only the resulting Boolean expression, just like generated columns.
fn size_predicate(max_bytes: usize) -> Expr {
    create_udf(
        "within_size_limit",
        vec![DataType::Binary],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let keep: BooleanArray = arrays[0]
                .as_binary::<i32>()
                .iter()
                .map(|value| value.map(|value| value.len() <= max_bytes))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(keep)))
        }),
    )
    .call(vec![col("data")])
}

#[tokio::test]
async fn caller_udf_reads_hidden_dependency_and_supports_empty_projection() {
    let memory = MemoryRecordAccessor::new(vec![batch()]);
    let source = memory
        .clone()
        .layer(FilterLayer::new(vec![size_predicate(3)]));
    let request = ReadRequest::new("input").with_projection(["id"]);
    let metadata = source.metadata(request.clone()).await.unwrap();
    let reader = source.open(request).await.unwrap();
    assert_eq!(*reader.metadata(), metadata);
    assert_eq!(collect_column::<Int64Type>(reader, 0).await, [0, 1, 2, 4]);
    assert_eq!(
        memory.projections.lock().unwrap().last(),
        Some(&Some(vec!["id".into(), "data".into()]))
    );
    let mut reader = source
        .open(ReadRequest::new("input").with_projection(Vec::<String>::new()))
        .await
        .unwrap();
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!((output.num_columns(), output.num_rows()), (0, 4));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn scalar_literals_and_udfs_expand_to_the_batch_row_count() {
    for (value, expected_rows) in [(Some(true), 6), (Some(false), 0), (None, 0)] {
        let scalar = datafusion::common::ScalarValue::Boolean(value);
        let result = scalar.clone();
        let udf = create_udf(
            "constant",
            vec![DataType::Int64],
            DataType::Boolean,
            Volatility::Stable,
            Arc::new(move |_| Ok(ColumnarValue::Scalar(result.clone()))),
        );
        for predicate in [lit(scalar), udf.call(vec![col("id")])] {
            let memory = MemoryRecordAccessor::new(vec![batch().slice(0, 0), batch()]);
            let source = memory.layer(FilterLayer::new(vec![predicate]));
            let batches: Vec<_> =
                into_stream(source.open(ReadRequest::new("input")).await.unwrap())
                    .try_collect()
                    .await
                    .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows
            );
            for batch in &batches {
                assert_gt!(batch.num_rows(), 0);
            }
        }
    }
}

#[tokio::test]
async fn empty_vector_preserves_batches() {
    let empty = batch().slice(0, 0);
    let source =
        MemoryRecordAccessor::new(vec![empty.clone(), batch()]).layer(FilterLayer::default());
    let mut reader = source.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(reader.next().await.unwrap(), Some(empty));
    assert_eq!(reader.next().await.unwrap(), Some(batch()));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_predicates_fail_before_reading() {
    for predicate in [
        col("missing").eq(lit(1i64)),
        col("id"),
        datafusion::functions_aggregate::expr_fn::sum(col("id")),
    ] {
        let memory = MemoryRecordAccessor::new(vec![batch()]);
        let source = memory.clone().layer(FilterLayer::new(vec![predicate]));
        let error = source.open(ReadRequest::new("input")).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(source.metadata(ReadRequest::new("input")).await.is_err());
        assert_eq!(memory.polls.load(Ordering::SeqCst), 0);
    }
}

fn cell(column: &str, predicate: Expr) -> open_arrow_access_layers::FilterRule {
    open_arrow_access_layers::FilterRule::Cell {
        column: column.into(),
        predicate,
    }
}

#[tokio::test]
async fn cell_rules_preserve_rows_and_existing_nulls() {
    let source =
        MemoryRecordAccessor::new(vec![batch()]).layer(FilterLayer::with_rules(vec![cell(
            "data",
            size_predicate(2),
        )]));
    let request = ReadRequest::new("input");
    let metadata = source.metadata(request.clone()).await.unwrap();
    assert_eq!(
        source.inspect_metadata(request.clone()).await.unwrap(),
        Some(metadata.clone())
    );
    let mut reader = source.open(request).await.unwrap();
    assert_eq!(*reader.metadata(), metadata);
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.num_rows(), 6);
    assert_eq!(output.column(0), batch().column(0));
    assert_eq!(output.schema(), metadata.schema);
    assert_eq!(
        output
            .column(1)
            .as_binary::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some(b"".as_slice()),
            None,
            Some(b"ab".as_slice()),
            None,
            Some(b"x".as_slice()),
            None
        ]
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn mixed_rules_use_original_values_and_combine_cell_masks() {
    use open_arrow_access_layers::FilterRule;
    let source = MemoryRecordAccessor::new(vec![batch()]).layer(FilterLayer::with_rules(vec![
        cell("id", col("id").gt(lit(1i64))),
        // This must observe id=1 even though the preceding rule masks it.
        FilterRule::Row(col("id").gt(lit(0i64))),
        cell("id", col("id").lt(lit(5i64))),
        // This must also observe original ids, independently of the id masks.
        cell("data", col("id").lt(lit(3i64))),
    ]));
    let mut reader = source.open(ReadRequest::new("input")).await.unwrap();
    assert!(reader.metadata().schema.field(0).is_nullable());
    let output = reader.next().await.unwrap().unwrap();
    assert_eq!(output.num_rows(), 5);
    assert_eq!(
        output
            .column(0)
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [None, Some(2), Some(3), Some(4), None]
    );
    assert_eq!(
        output
            .column(1)
            .as_binary::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some(b"\xff\x00\x80".as_slice()),
            Some(b"ab".as_slice()),
            None,
            None,
            None
        ]
    );
}

#[tokio::test]
async fn invalid_cell_rules_fail_before_reading_even_for_hidden_targets() {
    for rule in [
        cell("missing", lit(true)),
        cell("id", col("missing").eq(lit(0i64))),
        cell("id", col("id")),
        cell(
            "id",
            datafusion::functions_aggregate::expr_fn::sum(col("id")),
        ),
    ] {
        let memory = MemoryRecordAccessor::new(vec![batch()]);
        let source = memory.clone().layer(FilterLayer::with_rules(vec![rule]));
        let request = ReadRequest::new("input").with_projection(["data"]);
        let error = source.open(request.clone()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(source.metadata(request.clone()).await.is_err());
        assert!(source.inspect_metadata(request).await.is_err());
        assert_eq!(memory.polls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn scan_partitions_share_nullable_output_schema_and_remain_lazy() {
    use open_arrow_access_core::{
        RecordCapability, RecordReader, Result, ScanPlan, ScanPlanner, async_trait,
    };
    use open_arrow_access_layers::FilterRule;

    struct ScanSource(MemoryRecordAccessor);
    #[async_trait]
    impl Accessor for ScanSource {
        type Request = ReadRequest;
        type Reader = dyn RecordReader;
        fn capability(&self) -> RecordCapability {
            self.0.capability()
        }
        async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
            self.0.open(request).await
        }
    }
    #[async_trait]
    impl ScanPlanner for ScanSource {
        async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
            let metadata = self.0.metadata(request.clone()).await?;
            let partitions = (0..2)
                .map(|_| {
                    let source = self.0.clone();
                    let request = request.clone();
                    Box::pin(async move { source.open(request).await })
                        as open_arrow_access_core::ScanPartition
                })
                .collect();
            Ok(ScanPlan::new(metadata, partitions))
        }
    }

    let memory = MemoryRecordAccessor::new(vec![batch()]);
    let source = ScanSource(memory.clone()).layer(FilterLayer::with_rules(vec![
        FilterRule::Row(col("id").gt(lit(0i64))),
        cell("id", col("id").lt(lit(3i64))),
    ]));
    let plan = source
        .plan_scan(ReadRequest::new("input").with_projection(["id"]))
        .await
        .unwrap();
    assert_eq!(memory.polls.load(Ordering::SeqCst), 0);
    assert!(plan.metadata.schema.field(0).is_nullable());
    let expected_schema = plan.metadata.schema.clone();
    let batches: Vec<_> = into_stream(Box::new(plan.into_reader()))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 2);
    for output in batches {
        assert_eq!(output.schema(), expected_schema);
        assert_eq!(
            output
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(1), Some(2), None, None, None]
        );
    }
}

#[tokio::test]
async fn cell_rules_reject_layouts_without_top_level_validity() {
    use arrow_array::{Int32Array, RunArray, UnionArray, types::Int32Type};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{Field, UnionFields};

    let union = UnionArray::try_new(
        UnionFields::try_new([0], [Field::new("value", DataType::Int64, false)]).unwrap(),
        ScalarBuffer::from(vec![0i8]),
        Some(ScalarBuffer::from(vec![0i32])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let run =
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1]), &Int64Array::from(vec![1]))
            .unwrap();
    for array in [Arc::new(union) as ArrayRef, Arc::new(run)] {
        let batch = RecordBatch::try_from_iter([("value", array)]).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let memory = MemoryRecordAccessor::new(vec![batch]);
        let source = memory
            .clone()
            .layer(FilterLayer::with_rules(vec![cell("value", lit(false))]));
        let error = source.open(ReadRequest::new("input")).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("unsupported type"));
        assert_eq!(memory.polls.load(Ordering::SeqCst), 0);
    }
}
