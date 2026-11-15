use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::record_batch;
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, EmissionPolicy, ErrorKind, OrderRequirement, ParallelReadOptions, PreparedSource,
    ReadOptions, ReadRequest, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, StreamReader, async_trait, project_batch,
};

#[derive(Clone)]
struct Dataset {
    batch: Arc<Mutex<RecordBatch>>,
    requests: Arc<Mutex<Vec<ReadRequest>>>,
}

impl Dataset {
    fn new() -> Self {
        let batch = record_batch!(("id", Int32, [1, 2]), ("label", Utf8, ["a", "b"])).unwrap();
        Self {
            batch: Arc::new(Mutex::new(batch)),
            requests: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for Dataset {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.requests.lock().unwrap().push(request.clone());
        let batch = self.batch.lock().unwrap().clone();
        let (schema, projection) = request.options.resolve_projection(&batch.schema())?;
        let batch = project_batch(batch, projection.as_deref(), schema.clone())?;
        Ok(Box::new(StreamReader::records(
            stream::iter([Ok(batch)]),
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            OrderRequirement::PreserveGlobal,
        )))
    }
}

#[async_trait]
impl ScanPlanner for Dataset {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        self.requests.lock().unwrap().push(request.clone());
        let (schema, _) = request
            .options
            .resolve_projection(&self.batch.lock().unwrap().schema())?;
        let dataset = self.clone();
        let source = request.object.clone();
        let partitions = stream::iter(0..2).map(move |_| {
            let dataset = dataset.clone();
            let request = request.clone();
            Ok(Box::pin(async move { dataset.open(request).await }) as ScanPartition)
        });
        let mut plan = ScanPlan::from_stream(
            RecordMetadata {
                schema,
                source: Some(source),
            },
            partitions,
        )
        .with_ordering(OrderRequirement::Reorderable);
        plan.parallel_read = Some(ParallelReadOptions {
            partitions: NonZeroUsize::new(2).unwrap(),
            prefetch: NonZeroUsize::new(1).unwrap(),
        });
        Ok(plan)
    }
}

#[tokio::test]
async fn scan_applies_execution_options_to_the_bound_selection() {
    let dataset = Dataset::new();
    let options = ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: Some(vec!["label".into()]),
    };
    let prepared = PreparedSource::prepare(
        dataset.clone(),
        ReadRequest::new("dataset").with_options(options.clone()),
    )
    .await
    .unwrap();
    assert_eq!(prepared.schema().fields().len(), 2);
    assert!(
        dataset.requests.lock().unwrap()[0]
            .options
            .projection
            .is_none()
    );

    let mut reader = prepared.scan(options.clone()).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "label");
    assert_eq!(batch.num_rows(), 2);
    let requests = dataset.requests.lock().unwrap();
    assert_eq!(requests[1].object.locator(), "dataset");
    assert_eq!(requests[1].options.batch_size, options.batch_size);
    assert_eq!(requests[1].options.projection, options.projection);
}

#[tokio::test]
async fn compatible_scan_reuses_the_cached_reader_once() {
    let dataset = Dataset::new();
    let prepared = PreparedSource::prepare(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    prepared
        .scan(ReadOptions {
            projection: Some(vec![]),
            ..ReadOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 2);

    let mut reader = prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 2);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn scan_rejects_schema_changes_after_preparation() {
    let dataset = Dataset::new();
    let prepared = PreparedSource::prepare(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    {
        let mut batch = dataset.batch.lock().unwrap();
        let schema = Arc::new(
            batch
                .schema()
                .as_ref()
                .clone()
                .with_metadata([("version".into(), "changed".into())].into()),
        );
        *batch = RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap();
    }
    let error = prepared
        .scan(ReadOptions {
            projection: Some(vec!["label".into()]),
            ..ReadOptions::default()
        })
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
}

#[tokio::test]
async fn planning_preserves_projection_and_policies_and_opens_partitions_lazily() {
    let dataset = Dataset::new();
    let prepared = PreparedSource::prepare_plannable(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    let options = ReadOptions {
        projection: Some(vec!["label".into()]),
        ..ReadOptions::default()
    };
    let mut plan = prepared.plan_scan(options.clone()).await.unwrap();
    assert_eq!(plan.metadata.schema.field(0).name(), "label");
    assert_eq!(plan.ordering, OrderRequirement::PreserveGlobal);
    assert_eq!(plan.emission, EmissionPolicy::Ordered);
    assert_eq!(plan.parallel_read.unwrap().partitions.get(), 2);
    assert_eq!(plan.parallel_read.unwrap().prefetch.get(), 1);
    assert_eq!(dataset.requests.lock().unwrap().len(), 2); // bind + plan, no partition
    let planned = dataset.requests.lock().unwrap()[1].clone();
    assert_eq!(planned.object.locator(), "dataset");
    assert_eq!(planned.options.projection, options.projection);
    let partition = plan.partitions.next().await.unwrap().unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 2);
    let mut reader = partition.await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 3);
    // Planning leaves BoundSource's initial reader available for a normal scan.
    prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 3);
    drop(prepared);
    assert_eq!(reader.next().await.unwrap().unwrap().num_columns(), 1);
}

#[tokio::test]
async fn ordinary_prepare_returns_unsupported_and_invalid_projection_does_not_plan() {
    let dataset = Dataset::new();
    let prepared = PreparedSource::prepare(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    let error = prepared
        .plan_scan(ReadOptions::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(dataset.requests.lock().unwrap().len(), 1);
    let prepared = PreparedSource::prepare_plannable(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    let error = prepared
        .plan_scan(ReadOptions {
            projection: Some(vec!["missing".into()]),
            ..ReadOptions::default()
        })
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(dataset.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn planning_and_partition_opening_reject_schema_drift() {
    let dataset = Dataset::new();
    let original = dataset.batch.lock().unwrap().clone();
    let prepared = PreparedSource::prepare_plannable(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    let changed = Arc::new(
        original
            .schema()
            .as_ref()
            .clone()
            .with_metadata([("version".into(), "changed".into())].into()),
    );
    let changed = RecordBatch::try_new(changed, original.columns().to_vec()).unwrap();
    *dataset.batch.lock().unwrap() = changed.clone();
    let error = prepared
        .plan_scan(ReadOptions::default())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    *dataset.batch.lock().unwrap() = original;
    let mut plan = prepared.plan_scan(ReadOptions::default()).await.unwrap();
    *dataset.batch.lock().unwrap() = changed;
    let error = plan
        .partitions
        .next()
        .await
        .unwrap()
        .unwrap()
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
}
