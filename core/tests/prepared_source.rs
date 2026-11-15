use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, Error, ErrorKind, OrderRequirement, PreparedSource, ReadOptions,
    ReadOptionsProvider, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result,
    StreamReader, async_trait, project_batch,
};

#[derive(Clone, Debug)]
struct DatasetRequest {
    selection: String,
    provider_option: bool,
    byte_options: ByteReadOptions,
    options: ReadOptions,
}

impl ReadOptionsProvider for DatasetRequest {
    fn read_options(&self) -> &ReadOptions {
        &self.options
    }

    fn read_options_mut(&mut self) -> &mut ReadOptions {
        &mut self.options
    }
}

#[derive(Clone, Copy, Default)]
enum Failure {
    #[default]
    None,
    MetadataDrift,
    BatchDrift,
    Open,
    Read,
}

#[derive(Clone, Default)]
struct Dataset {
    requests: Arc<Mutex<Vec<DatasetRequest>>>,
    failure: Failure,
    drops: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ])
    .with_metadata([("version".into(), "bound".into())].into());
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

fn request() -> DatasetRequest {
    DatasetRequest {
        selection: "owner/dataset@revision/config/split".into(),
        provider_option: true,
        byte_options: ByteReadOptions {
            io_concurrency: NonZeroUsize::new(3).unwrap(),
            io_chunk_size: Some(NonZeroUsize::new(1024).unwrap()),
        },
        options: ReadOptions::default(),
    }
}

fn options(columns: &[&str]) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: Some(columns.iter().map(|name| (*name).into()).collect()),
    }
}

#[async_trait]
impl Accessor for Dataset {
    type Request = DatasetRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }

    async fn open(&self, request: DatasetRequest) -> Result<Box<dyn RecordReader>> {
        let reopened = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len() > 1
        };
        if reopened && matches!(self.failure, Failure::Open) {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "dataset_open",
                "sentinel",
            ));
        }
        let batch = batch();
        let (mut schema, projection) = request.options.resolve_projection(&batch.schema())?;
        let batch = project_batch(batch, projection.as_deref(), schema.clone())?;
        if reopened && matches!(self.failure, Failure::MetadataDrift) {
            schema = Arc::new(schema.as_ref().clone().with_metadata(Default::default()));
        }
        let batch = if reopened && matches!(self.failure, Failure::BatchDrift) {
            let wrong = Arc::new(schema.as_ref().clone().with_metadata(Default::default()));
            RecordBatch::try_new(wrong, batch.columns().to_vec()).unwrap()
        } else {
            batch
        };
        let item = if reopened && matches!(self.failure, Failure::Read) {
            Err(Error::new(ErrorKind::Timeout, "dataset_next", "sentinel"))
        } else {
            Ok(batch)
        };
        let guard = DropCounter(self.drops.clone());
        let polls = self.polls.clone();
        let stream = stream::iter(vec![item]).inspect(move |_| {
            let _ = &guard;
            polls.fetch_add(1, Ordering::SeqCst);
        });
        Ok(Box::new(StreamReader::records(
            stream,
            RecordMetadata {
                schema,
                source: Some(request.selection.into()),
            },
            OrderRequirement::Reorderable,
        )))
    }
}

#[tokio::test]
async fn scan_changes_only_read_options_and_preserves_projection_order() {
    let dataset = Dataset::default();
    let mut initial_request = request();
    initial_request.options.projection = Some(vec!["label".into()]);
    let prepared = PreparedSource::prepare(dataset.clone(), initial_request.clone())
        .await
        .unwrap();
    assert_eq!(prepared.schema(), batch().schema());
    assert_eq!(dataset.polls.load(Ordering::SeqCst), 0);
    assert!(
        dataset.requests.lock().unwrap()[0]
            .options
            .projection
            .is_none()
    );
    let scan_options = options(&["label", "id"]);
    let mut reader = prepared.scan(scan_options.clone()).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.schema().field(0).name(), "label");
    assert_eq!(batch.schema().field(1).name(), "id");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(reader.capability().ordering, OrderRequirement::Reorderable);
    let requests = dataset.requests.lock().unwrap();
    let opened = &requests[1];
    assert_eq!(opened.selection, initial_request.selection);
    assert_eq!(opened.provider_option, initial_request.provider_option);
    assert_eq!(opened.byte_options, initial_request.byte_options);
    assert_eq!(opened.options.batch_size, scan_options.batch_size);
    assert_eq!(opened.options.projection, scan_options.projection);
}

#[tokio::test]
async fn incompatible_scans_leave_initial_reader_for_a_compatible_scan() {
    let dataset = Dataset::default();
    let prepared = PreparedSource::prepare(dataset.clone(), request())
        .await
        .unwrap();
    let mut projected = prepared.scan(options(&["label"])).await.unwrap();
    let mut first = prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 2);
    let mut second = prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 3);
    drop(prepared);
    for reader in [&mut first, &mut second, &mut projected] {
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 3);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn batch_size_changes_reopen_and_empty_projection_preserves_rows() {
    let dataset = Dataset::default();
    let prepared = PreparedSource::prepare(dataset.clone(), request())
        .await
        .unwrap();
    let changed_batch_size = ReadOptions {
        batch_size: NonZeroUsize::new(17).unwrap(),
        projection: None,
    };
    prepared.scan(changed_batch_size.clone()).await.unwrap();
    assert_eq!(
        dataset.requests.lock().unwrap()[1].options.batch_size,
        changed_batch_size.batch_size
    );
    let mut reader = prepared.scan(options(&[])).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 3);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_projection_fails_before_opening_and_retains_initial_reader() {
    let dataset = Dataset::default();
    let prepared = PreparedSource::prepare(dataset.clone(), request())
        .await
        .unwrap();
    for columns in [&["missing"][..], &["id", "id"][..]] {
        let error = prepared.scan(options(columns)).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    prepared.scan(ReadOptions::default()).await.unwrap();
    assert_eq!(dataset.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn metadata_and_batch_drift_are_rejected_against_projected_bound_schema() {
    for failure in [Failure::MetadataDrift, Failure::BatchDrift] {
        let dataset = Dataset {
            failure,
            ..Default::default()
        };
        let prepared = PreparedSource::prepare(dataset.clone(), request())
            .await
            .unwrap();
        let scan = prepared.scan(options(&["label"])).await;
        if matches!(failure, Failure::MetadataDrift) {
            assert_eq!(scan.err().unwrap().kind(), ErrorKind::Decode);
        } else {
            let mut reader = scan.unwrap();
            assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
            assert!(reader.next().await.unwrap().is_none());
            // The failing scan releases its stream without waiting for the wrapper to drop.
            assert_eq!(dataset.drops.load(Ordering::SeqCst), 1);
            assert_eq!(reader.metadata().schema.field(0).name(), "label");
        }
    }
}

#[tokio::test]
async fn source_open_and_read_errors_keep_their_categories_and_context() {
    for failure in [Failure::Open, Failure::Read] {
        let dataset = Dataset {
            failure,
            ..Default::default()
        };
        let prepared = PreparedSource::prepare(dataset, request()).await.unwrap();
        let scan = prepared.scan(options(&["id"])).await;
        if matches!(failure, Failure::Open) {
            let error = scan.err().unwrap();
            assert_eq!(error.kind(), ErrorKind::PermissionDenied);
            assert_eq!(error.operation(), "dataset_open");
        } else {
            let mut reader = scan.unwrap();
            let error = reader.next().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Timeout);
            assert_eq!(error.operation(), "dataset_next");
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}
