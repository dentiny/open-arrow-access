use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteReadOptions, ByteReader, Capability, ErrorKind, FactoryToken, OrderRequirement,
    OuterCompression, ReadOptions, ReadRequest, ReaderFactory, RecordCapability, RecordReader,
    Result, SchemaRef, async_trait, open_file_source,
};
use open_arrow_access_test_support::{MemoryRecordAccessor, fixture_path, fs_operator};

#[derive(Clone)]
struct ProbeFormat {
    schema: Arc<Mutex<SchemaRef>>,
    requests: Arc<Mutex<Vec<ReadRequest>>>,
    opens: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}

impl ProbeFormat {
    fn new() -> Self {
        Self {
            schema: Arc::new(Mutex::new(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("hidden", DataType::Int64, false),
            ])))),
            requests: Arc::default(),
            opens: Arc::default(),
            polls: Arc::default(),
        }
    }
}

#[async_trait]
impl ReaderFactory for ProbeFormat {
    type Request = ReadRequest;
    type Prepared = SchemaRef;
    type Reader = dyn RecordReader;

    fn capability(&self, _: Capability) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::Reorderable)
    }

    fn prepare(&self, request: &ReadRequest) -> Result<SchemaRef> {
        self.requests.lock().unwrap().push(request.clone());
        let schema = self.schema.lock().unwrap().clone();
        request.options.resolve_projection(&schema)?;
        Ok(schema)
    }

    async fn create_reader(
        &self,
        _: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ReadRequest,
        schema: SchemaRef,
    ) -> Result<Box<dyn RecordReader>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        assert_eq!(input.metadata().source.as_ref(), Some(&request.object));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut records = MemoryRecordAccessor::new(vec![batch]);
        records.polls = self.polls.clone();
        records.open(request).await
    }
}

#[tokio::test]
async fn first_reader_is_reused_reopens_are_independent_and_schema_drift_is_rejected() {
    let format = ProbeFormat::new();
    let path = fixture_path!("json", "good.json");
    let request = ReadRequest::new("good.json")
        .with_options(ReadOptions {
            batch_size: 2.try_into().unwrap(),
            projection: Some(vec!["id".into()]),
        })
        .with_byte_options(ByteReadOptions {
            io_concurrency: 3.try_into().unwrap(),
            io_chunk_size: Some(7.try_into().unwrap()),
        });
    let source = open_file_source(
        fs_operator(path.parent().unwrap()),
        request.clone(),
        format.clone(),
        OuterCompression::None,
    )
    .await
    .unwrap();
    assert_eq!(format.opens.load(Ordering::SeqCst), 1);
    assert_eq!(source.schema().fields().len(), 1);
    assert_eq!(source.metadata().source.as_ref(), Some(&request.object));
    assert_eq!(format.polls.load(Ordering::SeqCst), 0);

    fn require_send<T: Send>(value: T) -> T {
        value
    }
    let (first, second) =
        tokio::join!(require_send(source.reader()), require_send(source.reader()));
    let (mut first, mut second) = (first.unwrap(), second.unwrap());
    assert_eq!(format.opens.load(Ordering::SeqCst), 2);
    assert_eq!(format.polls.load(Ordering::SeqCst), 0);
    assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 2);

    let original = source.schema();
    let changed = format
        .schema
        .lock()
        .unwrap()
        .as_ref()
        .clone()
        .with_metadata([("changed".into(), "yes".into())].into());
    *format.schema.lock().unwrap() = Arc::new(changed);
    let error = source.reader().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Decode);
    assert_eq!(error.operation(), "file_source_reader");
    assert_eq!(source.schema(), original);
    for seen in format.requests.lock().unwrap().iter() {
        assert_eq!(seen.object, request.object);
        assert_eq!(seen.options.projection, request.options.projection);
        assert_eq!(seen.options.batch_size, request.options.batch_size);
        assert_eq!(seen.byte_options, request.byte_options);
    }
    drop(source);
    assert_eq!(second.next().await.unwrap().unwrap().num_rows(), 2);
    assert!(first.next().await.unwrap().is_none());
    assert!(second.next().await.unwrap().is_none());
}

#[tokio::test]
async fn preparation_and_storage_errors_keep_their_categories() {
    let dir = tempfile::tempdir().unwrap();
    let format = ProbeFormat::new();
    let error = open_file_source(
        fs_operator(dir.path()),
        ReadRequest::new("missing.json").with_options(ReadOptions {
            projection: Some(vec!["missing_field".into()]),
            ..ReadOptions::default()
        }),
        format.clone(),
        OuterCompression::None,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(format.opens.load(Ordering::SeqCst), 0);
    let error = open_file_source(
        fs_operator(dir.path()),
        ReadRequest::new("missing.json"),
        format,
        OuterCompression::None,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
}

#[tokio::test]
async fn auto_rejects_disabled_codecs_before_storage_opens() {
    // Other workspace packages can enable codecs through dependency unification.
    let Some(key) = [
        "missing.json.gz",
        "missing.json.bz2",
        "missing.json.lz4",
        "missing.json.zst",
    ]
    .into_iter()
    .find(|key| open_arrow_access::decompression::Compression::from_path(key).is_err()) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let error = open_file_source(
        fs_operator(dir.path()),
        ReadRequest::new(key),
        ProbeFormat::new(),
        OuterCompression::Auto,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}
