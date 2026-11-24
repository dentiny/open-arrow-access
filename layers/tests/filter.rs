use std::sync::{Arc, atomic::Ordering};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, cast::AsArray, types::Int64Type,
};
use arrow_schema::DataType;
use datafusion::{
    logical_expr::{ColumnarValue, Expr, Volatility, create_udf},
    prelude::{col, lit},
};
use futures::{TryStreamExt, stream};
use open_arrow_access_core::*;
use open_arrow_access_layers::{
    ColumnEvolution, FilterLayer, ParallelReadLayer, SchemaEvolutionLayer,
};
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
    assert!(
        batches
            .iter()
            .all(|output| output.num_rows() > 0 && output.schema() == batch.schema())
    );
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
            assert!(batches.iter().all(|batch| batch.num_rows() > 0));
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

struct PlannedSource(MemoryRecordAccessor);
#[async_trait]
impl Accessor for PlannedSource {
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
impl ScanPlanner for PlannedSource {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = self.0.metadata(request.clone()).await?;
        let parts = (0..2)
            .map(|_| {
                let memory = self.0.clone();
                let request = request.clone();
                Box::pin(async move { memory.open(request).await }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(metadata, parts))
    }
}

#[tokio::test]
async fn filters_lazy_parallel_partitions_and_restores_projection() {
    let source = PlannedSource(MemoryRecordAccessor::new(vec![batch()]))
        .layer(FilterLayer::new(vec![size_predicate(3)]))
        .layer(ParallelReadLayer::new(
            std::num::NonZeroUsize::new(2).unwrap(),
            std::num::NonZeroUsize::MIN,
        ));
    let reader = source
        .open(ReadRequest::new("input").with_projection(["id"]))
        .await
        .unwrap();
    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        [0, 1, 2, 4, 0, 1, 2, 4]
    );
}

struct FailingSource;
#[async_trait]
impl Accessor for FailingSource {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }
    async fn open(&self, _: ReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(StreamReader::records(
            stream::iter(vec![
                Ok(batch()),
                Err(Error::new(ErrorKind::PermissionDenied, "source", "denied")),
                Ok(batch()),
            ]),
            RecordMetadata {
                schema: batch().schema(),
                source: Some(ObjectRef::new("input")),
            },
            OrderRequirement::PreserveGlobal,
        )))
    }
}

#[tokio::test]
async fn upstream_errors_keep_classification_and_are_terminal() {
    let source = FailingSource.layer(FilterLayer::new(vec![col("id").gt(lit(3i64))]));
    let mut reader = source.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.object().unwrap().locator(), "input");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn multi_column_udf_composes_with_generated_columns() {
    use std::sync::atomic::AtomicUsize;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let predicate = create_udf(
        "keep_pair",
        vec![DataType::Int64, DataType::Int64],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(move |args| {
            observed.fetch_add(1, Ordering::SeqCst);
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let keep: BooleanArray = arrays[0]
                .as_primitive::<Int64Type>()
                .iter()
                .zip(arrays[1].as_primitive::<Int64Type>().iter())
                .map(|(left, right)| left.zip(right).map(|(left, right)| left + right >= 5))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(keep)))
        }),
    );
    let memory = MemoryRecordAccessor::new(vec![batch().slice(0, 3), batch().slice(3, 3)]);
    let source = memory
        .clone()
        .layer(
            SchemaEvolutionLayer::try_new(vec![ColumnEvolution::add_column_expr(
                "next_id",
                col("id") + lit(1i64),
            )])
            .unwrap(),
        )
        .layer(FilterLayer::new(vec![
            predicate.call(vec![col("id"), col("next_id")]),
            col("id").lt(lit(5i64)),
        ]));
    let request = ReadRequest::new("input").with_projection(["id"]);
    let reader = source.open(request).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.metadata().schema.fields().len(), 1);
    assert_eq!(collect_column::<Int64Type>(reader, 0).await, [2, 3, 4]);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn malformed_or_failing_custom_predicate_terminates_reader() {
    use datafusion::error::DataFusionError;
    for output in [
        None,
        Some(Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
        Some(Arc::new(Int64Array::from(vec![0; 6])) as ArrayRef),
    ] {
        let udf = create_udf(
            "custom_filter",
            vec![DataType::Int64],
            DataType::Boolean,
            Volatility::Stable,
            Arc::new(move |_| {
                output
                    .clone()
                    .map(ColumnarValue::Array)
                    .ok_or_else(|| DataFusionError::Execution("predicate failed".into()))
            }),
        );
        let memory = MemoryRecordAccessor::new(vec![batch(), batch()]);
        let source = memory
            .clone()
            .layer(FilterLayer::new(vec![udf.call(vec![col("id")])]));
        let mut reader = source.open(ReadRequest::new("input")).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(memory.batches_read.load(Ordering::SeqCst), 1);
    }
}
