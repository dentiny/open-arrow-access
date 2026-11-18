use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::record_batch;
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ErrorKind, OrderRequirement, PreparedSource, ReadOptions, ReadRequest, Reader,
    RecordBatch, RecordCapability, RecordMetadata, RecordReader, ResolvedProjection, Result,
    ScanPartition, ScanPlan, ScanPlanner, ScanStatistics, Statistic, StatisticsProvider,
    StreamReader, async_trait, project_batch,
};

#[derive(Clone)]
struct Dataset {
    batch: Arc<Mutex<RecordBatch>>,
    requests: Arc<Mutex<Vec<ReadRequest>>>,
    stat_requests: Arc<Mutex<Vec<ReadRequest>>>,
    consumed: Arc<AtomicUsize>,
}

impl Dataset {
    fn new() -> Self {
        let batch = record_batch!(("id", Int32, [1, 2]), ("label", Utf8, ["a", "b"])).unwrap();
        Self {
            batch: Arc::new(Mutex::new(batch)),
            requests: Arc::default(),
            stat_requests: Arc::default(),
            consumed: Arc::default(),
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
        let consumed = self.consumed.clone();
        Ok(Box::new(StreamReader::records(
            stream::iter([Ok(batch)]).inspect(move |_| {
                consumed.fetch_add(1, Ordering::SeqCst);
            }),
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

#[async_trait]
impl StatisticsProvider for Dataset {
    async fn statistics(&self, request: ReadRequest) -> Result<ScanStatistics> {
        self.stat_requests.lock().unwrap().push(request.clone());
        if request.object.locator() == "fail" {
            return Err(open_arrow_access_core::Error::new(
                ErrorKind::Io,
                "statistics_test",
                "metadata failed",
            ));
        }
        Ok(ScanStatistics {
            row_count: Statistic::Estimated(2),
            source_bytes: Statistic::Exact(512),
        })
    }
}

#[rstest::rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn statistics_are_available_before_consumption_and_preserve_scan_options(
    #[case] plannable: bool,
) {
    let dataset = Dataset::new();
    let request = ReadRequest::new("dataset");
    let source = if plannable {
        PreparedSource::prepare_plannable_with_statistics(dataset.clone(), request).await
    } else {
        PreparedSource::prepare_with_statistics(dataset.clone(), request).await
    }
    .unwrap();
    // Preparing does not collect statistics; collecting does not poll record data.
    assert!(dataset.stat_requests.lock().unwrap().is_empty());
    let options = ReadOptions {
        projection: Some(vec!["label".into()]),
        batch_size: NonZeroUsize::new(7).unwrap(),
    };
    let stats = source.statistics(options.clone()).await.unwrap();
    assert_eq!(stats.row_count, Statistic::Estimated(2));
    assert_eq!(stats.source_bytes, Statistic::Exact(512));
    assert_eq!(dataset.consumed.load(Ordering::SeqCst), 0);
    {
        let requests = dataset.stat_requests.lock().unwrap();
        assert_eq!(requests[0].object.locator(), "dataset");
        assert_eq!(requests[0].options.projection, options.projection);
        assert_eq!(requests[0].options.batch_size, options.batch_size);
    }
    if plannable {
        source.plan_scan(options).await.unwrap();
    }
    // Inspection has not consumed the compatible cached reader or reopened it.
    let mut reader = source.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 1);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 2);
    assert_eq!(dataset.consumed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn statistics_validate_projection_and_propagate_provider_errors() {
    let dataset = Dataset::new();
    let source = PreparedSource::prepare_with_statistics(dataset.clone(), ReadRequest::new("fail"))
        .await
        .unwrap();
    assert!(
        source
            .statistics(ReadOptions {
                projection: Some(vec!["missing".into()]),
                ..Default::default()
            })
            .await
            .is_err()
    );
    assert!(dataset.stat_requests.lock().unwrap().is_empty());
    assert_eq!(
        source
            .statistics(ReadOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Io
    );
    assert_eq!(dataset.consumed.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preparation_without_statistics_retains_unknown_fallback() {
    let dataset = Dataset::new();
    let source = PreparedSource::prepare(dataset.clone(), ReadRequest::new("dataset"))
        .await
        .unwrap();
    assert_eq!(
        source.statistics(ReadOptions::default()).await.unwrap(),
        ScanStatistics::default()
    );
    assert!(dataset.stat_requests.lock().unwrap().is_empty());
    assert_eq!(dataset.consumed.load(Ordering::SeqCst), 0);
}
