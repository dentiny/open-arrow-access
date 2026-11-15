use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, ErrorKind, OrderRequirement, PreparedSource, ReadOptions, ReadRequest, RecordBatch,
    RecordCapability, RecordMetadata, RecordReader, Result, StreamReader, async_trait,
    project_batch,
};

#[derive(Clone)]
struct Dataset {
    batch: Arc<Mutex<RecordBatch>>,
    requests: Arc<Mutex<Vec<ReadRequest>>>,
}

impl Dataset {
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
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
