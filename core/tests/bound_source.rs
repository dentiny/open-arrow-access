use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, BoundSource, Error, ErrorKind, OrderRequirement, RecordBatch, RecordCapability,
    RecordMetadata, RecordReader, Result, StreamReader, async_trait,
};

#[derive(Clone, Debug, PartialEq)]
struct Selection {
    id: u64,
    revision: String,
}

#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<Selection>>,
    polls: Arc<AtomicUsize>,
    schema_revision: AtomicUsize,
    fail_open: Mutex<Option<ErrorKind>>,
}

#[async_trait]
impl Accessor for Probe {
    type Request = Selection;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: Selection) -> Result<Box<dyn RecordReader>> {
        self.requests.lock().unwrap().push(request);
        if let Some(kind) = *self.fail_open.lock().unwrap() {
            return Err(Error::new(kind, "probe_open", "source failed"));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int32, false)],
            [(
                "revision".into(),
                self.schema_revision.load(Ordering::SeqCst).to_string(),
            )]
            .into(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7]))])
            .unwrap();
        let polls = self.polls.clone();
        let stream = stream::iter([Ok(batch)]).inspect(move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
        });
        Ok(Box::new(StreamReader::records(
            stream,
            RecordMetadata {
                schema,
                source: Some("selected".into()),
            },
            OrderRequirement::PreserveGlobal,
        )))
    }
}

fn request() -> Selection {
    Selection {
        id: 42,
        revision: "original".into(),
    }
}

#[tokio::test]
async fn retains_initial_reader_and_reopens_independent_typed_selections() {
    let probe = Arc::new(Probe::default());
    let source = BoundSource::bind(probe.clone(), request()).await.unwrap();
    assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        source.metadata().source.as_ref().unwrap().locator(),
        "selected"
    );
    assert_eq!(source.schema(), source.metadata().schema);
    let mut first = source.reader().await.unwrap();
    assert_eq!(probe.requests.lock().unwrap().len(), 1);
    let (second, third) = tokio::join!(source.reader(), source.reader());
    let (mut second, mut third) = (second.unwrap(), third.unwrap());
    assert_eq!(*probe.requests.lock().unwrap(), vec![request(); 3]);
    drop(source);
    assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(first.next().await.unwrap().is_none());
    assert_eq!(second.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(third.next().await.unwrap().unwrap().num_rows(), 1);
}

#[tokio::test]
async fn rejects_schema_metadata_drift_before_polling_reopened_reader() {
    let probe = Arc::new(Probe::default());
    let source = BoundSource::bind(probe.clone(), request()).await.unwrap();
    drop(source.reader().await.unwrap());
    probe.schema_revision.store(1, Ordering::SeqCst);
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "bound_source_reader");
    assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn propagates_initial_and_reopen_errors_unchanged() {
    let probe = Arc::new(Probe::default());
    *probe.fail_open.lock().unwrap() = Some(ErrorKind::PermissionDenied);
    let error = BoundSource::bind(probe.clone(), request())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "probe_open");
    *probe.fail_open.lock().unwrap() = None;
    let source = BoundSource::bind(probe.clone(), request()).await.unwrap();
    drop(source.reader().await.unwrap());
    *probe.fail_open.lock().unwrap() = Some(ErrorKind::Unavailable);
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.operation(), "probe_open");
}
