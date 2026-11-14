use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, BoundSource, Error, ErrorKind, OrderRequirement, Reader, RecordBatch,
    RecordBatchBuilder, RecordCapability, RecordMetadata, RecordReader, Result, SchemaRef,
    async_trait,
};

// Deliberately neither ReadRequest nor ReadOptionsProvider.
#[derive(Clone, Debug, PartialEq)]
struct Request {
    selection: String,
    revision: u32,
    columns: Vec<String>,
}

fn request() -> Request {
    Request {
        selection: "partition/2026".into(),
        revision: 7,
        columns: vec!["id".into()],
    }
}

#[derive(Clone)]
struct Config {
    schema: SchemaRef,
    value: u64,
    open_error: bool,
    stream_error: bool,
}

struct State {
    config: Mutex<Config>,
    requests: Mutex<Vec<Request>>,
    polls: AtomicUsize,
    drops: AtomicUsize,
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            config: Mutex::new(Config {
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                value: 42,
                open_error: false,
                stream_error: false,
            }),
            requests: Mutex::new(Vec::new()),
            polls: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
        })
    }
}

// Deliberately not Clone: ownership must suffice.
struct TestAccessor(Arc<State>);

fn capability() -> RecordCapability {
    RecordCapability::sequential(OrderRequirement::PreserveGlobal)
}

#[async_trait]
impl Accessor for TestAccessor {
    type Request = Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        capability()
    }

    async fn open(&self, request: Request) -> Result<Box<dyn RecordReader>> {
        self.0.requests.lock().unwrap().push(request);
        let config = self.0.config.lock().unwrap().clone();
        tokio::task::yield_now().await;
        if config.open_error {
            return Err(Error::new(ErrorKind::Io, "test_open", "open failed"));
        }
        Ok(Box::new(TestReader {
            state: self.0.clone(),
            metadata: RecordMetadata {
                schema: config.schema,
                source: None,
            },
            value: config.value,
            stream_error: config.stream_error,
            done: false,
        }))
    }
}

struct TestReader {
    state: Arc<State>,
    metadata: RecordMetadata,
    value: u64,
    stream_error: bool,
    done: bool,
}

impl Drop for TestReader {
    fn drop(&mut self) {
        self.state.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for TestReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn capability(&self) -> RecordCapability {
        capability()
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.state.polls.fetch_add(1, Ordering::SeqCst);
        if self.done {
            return Ok(None);
        }
        self.done = true;
        if self.stream_error {
            return Err(Error::new(ErrorKind::Decode, "test_next", "stream failed"));
        }
        let mut batch = RecordBatchBuilder::new(self.metadata.schema.clone(), None)?;
        batch.push(|row| row.append("id", self.value))?;
        Ok(Some(batch.finish()?))
    }
}

#[tokio::test]
async fn retains_initial_reader_and_complete_typed_request() {
    let state = State::new();
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    assert_eq!(source.metadata().schema, source.schema());
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.requests.lock().unwrap().len(), 1);
    let mut first = source.reader().await.unwrap();
    assert_eq!(state.requests.lock().unwrap().len(), 1);
    let mut second = source.reader().await.unwrap();
    assert_eq!(*state.requests.lock().unwrap(), vec![request(), request()]);
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(first.next().await.unwrap(), second.next().await.unwrap());
    assert!(first.next().await.unwrap().is_none());
    assert!(second.next().await.unwrap().is_none());
}

#[tokio::test]
async fn concurrent_readers_claim_initial_reader_once_and_outlive_source() {
    let state = State::new();
    let source = Arc::new(
        BoundSource::bind(TestAccessor(state.clone()), request())
            .await
            .unwrap(),
    );
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let source = source.clone();
        tasks.push(tokio::spawn(async move { source.reader().await.unwrap() }));
    }
    let mut readers = Vec::new();
    for task in tasks {
        readers.push(task.await.unwrap());
    }
    drop(source);
    assert_eq!(state.requests.lock().unwrap().len(), 8);
    for mut reader in readers {
        assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(reader.next().await.unwrap().is_none());
    }
    assert_eq!(state.drops.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn drops_unused_initial_reader() {
    let state = State::new();
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    drop(source);
    assert_eq!(state.drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rejects_all_schema_drift_before_polling_and_can_retry() {
    let state = State::new();
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    drop(source.reader().await.unwrap());
    let original = source.schema();
    let variants = [
        Schema::new(vec![Field::new("renamed", DataType::UInt64, false)]),
        Schema::new(vec![Field::new("id", DataType::Int64, false)]),
        Schema::new(vec![Field::new("id", DataType::UInt64, true)]),
        Schema::new(vec![Field::new("id", DataType::UInt64, false)])
            .with_metadata([("version".into(), "2".into())].into()),
        Schema::new(vec![
            Field::new("id", DataType::UInt64, false)
                .with_metadata([("unit".into(), "items".into())].into()),
        ]),
    ];
    for schema in variants {
        state.config.lock().unwrap().schema = Arc::new(schema);
        let error = source.reader().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "bound_source_reader");
        assert_eq!(source.schema(), original);
        assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    }
    assert_eq!(state.drops.load(Ordering::SeqCst), 6);
    state.config.lock().unwrap().schema = original;
    assert!(source.reader().await.is_ok());
}

#[tokio::test]
async fn permits_new_contents_with_same_schema() {
    let state = State::new();
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    state.config.lock().unwrap().value = 99;
    let first = source
        .reader()
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let second = source
        .reader()
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.schema(), second.schema());
    assert_ne!(first, second);
}

#[tokio::test]
async fn propagates_bind_and_reopen_errors_and_allows_retry() {
    let state = State::new();
    state.config.lock().unwrap().open_error = true;
    let error = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "test_open");
    state.config.lock().unwrap().open_error = false;
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    state.config.lock().unwrap().open_error = true;
    // The cached reader is still available even when reopening would fail.
    drop(source.reader().await.unwrap());
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "test_open");
    state.config.lock().unwrap().open_error = false;
    assert!(source.reader().await.is_ok());
}

#[tokio::test]
async fn stream_errors_remain_deferred_and_unchanged() {
    let state = State::new();
    state.config.lock().unwrap().stream_error = true;
    let source = BoundSource::bind(TestAccessor(state.clone()), request())
        .await
        .unwrap();
    for _ in 0..2 {
        let mut reader = source.reader().await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "test_next");
    }
}

struct OtherAccessor(TestAccessor);

#[async_trait]
impl Accessor for OtherAccessor {
    type Request = (String, u32);
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        capability()
    }
    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        self.0
            .open(Request {
                selection: request.0,
                revision: request.1,
                columns: vec!["id".into()],
            })
            .await
    }
}

#[tokio::test]
async fn erases_different_request_types_and_accepts_dynamic_arc_accessors() {
    let state = State::new();
    let accessor: Arc<dyn Accessor<Request = Request, Reader = dyn RecordReader>> =
        Arc::new(TestAccessor(state.clone()));
    let sources = vec![
        BoundSource::bind(accessor, request()).await.unwrap(),
        BoundSource::bind(OtherAccessor(TestAccessor(state)), ("other".into(), 3))
            .await
            .unwrap(),
    ];
    for source in sources {
        assert_eq!(
            source
                .reader()
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
    }
}
