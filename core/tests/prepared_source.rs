use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::record_batch;
use futures::stream;
use open_arrow_access_core::{
    Accessor, ErrorKind, OrderRequirement, PreparedSource, ReadOptions, ReadRequest, Reader,
    RecordBatch, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
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

    /// Advertise the dataset's fixed global-order contract.
    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    /// Log this open and project the current batch into an independent reader.
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.requests.lock().unwrap().push(request.clone());
        let batch = self.batch.lock().unwrap().clone();
        let ResolvedProjection {
            schema,
            indices: projection,
        } = request.options.resolve_projection(&batch.schema())?;
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
    /// Expose two independent partitions without opening their readers.
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let schema = request
            .options
            .resolve_projection(&self.batch.lock().unwrap().schema())?
            .schema;
        let partitions = (0..2)
            .map(|_| {
                let dataset = self.clone();
                let request = request.clone();
                Box::pin(async move { dataset.open(request).await }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(
            RecordMetadata {
                schema,
                source: Some(request.object),
            },
            partitions,
        ))
    }
}

#[tokio::test]
async fn scan_applies_execution_options_to_the_bound_selection() {
    let dataset = Dataset::new();
    let options = ReadOptions {
        source_columns: Vec::new(),
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
async fn prepared_source_retains_lazy_projected_partitions() {
    let dataset = Dataset::new();
    let prepared = PreparedSource::prepare_plannable(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    let plan = prepared
        .plan_scan(ReadOptions {
            projection: Some(vec!["label".into()]),
            ..ReadOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 1);
    let mut reader = plan.into_reader();
    for _ in 0..2 {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.schema().field(0).name(), "label");
        assert_eq!(batch.num_columns(), 1);
    }
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(dataset.requests.lock().unwrap().len(), 3);
}
