use open_arrow_access_dataset::{
    directory::{DirectoryAccessor, DirectoryRequest},
    file_set::{FileSetAccessor, FileSetRequest},
};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{ArrayRef, Int64Array, types::Int64Type};
use arrow_schema::{DataType, Field};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind,
    ListRequest, ObjectRef, ObjectStream, OrderRequirement, ReadOptions, ReadOptionsProvider,
    ReadRequest, Reader, RecordBatch, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, Schema, SchemaRef, async_trait,
};
use open_arrow_access_layers::ParallelReadLayer;

fn schema(label: &str) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        [("label".into(), label.into())].into(),
    ))
}

fn batch(schema: SchemaRef, ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
    )
    .unwrap()
}

#[derive(Clone)]
struct FileData {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl FileData {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

#[derive(Clone)]
struct Files {
    files: HashMap<String, FileData>,
    opens: Arc<Mutex<Vec<ReadRequest>>>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl Files {
    fn new(files: impl IntoIterator<Item = (&'static str, FileData)>) -> Self {
        Self {
            files: files
                .into_iter()
                .map(|(name, file)| (name.into(), file))
                .collect(),
            opens: Arc::default(),
            active: Arc::default(),
            max_active: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for Files {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.opens.lock().unwrap().push(request.clone());
        let file = self
            .files
            .get(request.object.locator())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "open_fixture", "missing"))?;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        Ok(Box::new(Batches {
            active: self.active.clone(),
            metadata: RecordMetadata {
                schema: file.schema.clone(),
                source: Some(request.object),
            },
            batches: file.batches.clone().into_iter(),
        }))
    }
}

struct Batches {
    active: Arc<AtomicUsize>,
    metadata: RecordMetadata,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl Drop for Batches {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for Batches {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;
    fn capability(&self) -> RecordCapability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(OrderRequirement::PreserveGlobal)
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

async fn values(reader: Box<dyn RecordReader>) -> Vec<i64> {
    open_arrow_access_test_support::record::collect_column::<Int64Type>(reader, 0).await
}

#[tokio::test]
async fn file_set_keeps_explicit_order_duplicates_and_opens_lazily() {
    let schema = schema("same");
    let files = Files::new([
        (
            "a",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[1, 2])]),
        ),
        (
            "b",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[3])]),
        ),
    ]);
    let opens = files.opens.clone();
    let active = files.active.clone();
    let max_active = files.max_active.clone();
    let accessor = FileSetAccessor::new(files);
    let mut request = FileSetRequest::new(["b", "a", "b"]);
    request.read_options_mut().batch_size = NonZeroUsize::new(2).unwrap();
    request.byte_options.io_concurrency = NonZeroUsize::new(3).unwrap();
    let mut reader = accessor.open(request).await.unwrap();
    assert_eq!(opens.lock().unwrap().len(), 1);
    assert_eq!(reader.metadata().schema, schema);
    assert!(reader.metadata().source.is_none());
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(opens.lock().unwrap().len(), 1);
    assert_eq!(values(reader).await, vec![1, 2, 3]);
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let opens = opens.lock().unwrap();
    assert_eq!(
        opens.iter().map(|r| r.object.locator()).collect::<Vec<_>>(),
        ["b", "a", "b"]
    );
    assert!(
        opens
            .iter()
            .all(|r| r.options.batch_size.get() == 2 && r.byte_options.io_concurrency.get() == 3)
    );
}

#[tokio::test]
async fn file_set_rejects_empty_selection_and_skips_empty_files() {
    let schema = schema("same");
    let files = Files::new([
        ("empty", FileData::new(schema.clone(), vec![])),
        (
            "data",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[7])]),
        ),
    ]);
    let accessor = FileSetAccessor::new(files);
    assert_eq!(
        accessor
            .open(FileSetRequest::new(Vec::<ObjectRef>::new()))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        values(
            accessor
                .open(FileSetRequest::new(["empty", "data", "empty"]))
                .await
                .unwrap()
        )
        .await,
        [7]
    );
    let mut reader = accessor.open(FileSetRequest::new(["empty"])).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn later_schema_metadata_mismatch_and_open_failure_are_terminal() {
    let first = schema("first");
    let other = schema("other");
    let files = Files::new([
        (
            "first",
            FileData::new(first.clone(), vec![batch(first.clone(), &[1])]),
        ),
        ("mismatch", FileData::new(other, vec![])),
    ]);
    let opens = files.opens.clone();
    let accessor = FileSetAccessor::new(files);
    let mut reader = accessor
        .open(FileSetRequest::new(["first", "mismatch", "first"]))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "read_file_set");
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(opens.lock().unwrap().len(), 2);
    let mut reader = accessor
        .open(FileSetRequest::new(["first", "missing", "first"]))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(error.operation(), "open_fixture");
    let calls = opens.lock().unwrap().len();
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(opens.lock().unwrap().len(), calls);
}

#[tokio::test]
async fn batch_schema_is_checked_against_the_opened_schema() {
    let declared = schema("declared");
    let other = schema("other");
    let files = Files::new([("bad", FileData::new(declared, vec![batch(other, &[1])]))]);
    let mut reader = FileSetAccessor::new(files)
        .open(FileSetRequest::new(["bad"]))
        .await
        .unwrap();
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "read_file_set");
    assert!(reader.next().await.unwrap().is_none());
}

struct Listing {
    expected_prefix: Option<&'static str>,
    objects: Vec<&'static str>,
    fail: bool,
}

#[tokio::test]
async fn union_null_fills_report_run_end_overflow_and_terminate() {
    use arrow_schema::{UnionFields, UnionMode};
    let encoded = DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int16, false)),
        Arc::new(Field::new("values", DataType::Int64, true)),
    );
    let item = Arc::new(Field::new("item", encoded.clone(), true));
    let types = [
        encoded,
        DataType::Struct(vec![item.clone()].into()),
        DataType::FixedSizeList(item.clone(), 2),
        DataType::Union(
            UnionFields::try_new(
                [0, 1],
                [Arc::new(Field::new("plain", DataType::Int64, true)), item],
            )
            .unwrap(),
            UnionMode::Sparse,
        ),
    ];
    for data_type in types {
        for replace_null in [false, true] {
            let id = Field::new("id", DataType::Int64, false);
            let target = Arc::new(Schema::new(vec![
                id.clone(),
                Field::new("encoded", data_type.clone(), true),
            ]));
            let mut fields = vec![id];
            let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![0; 32768]))];
            if replace_null {
                fields.push(Field::new("encoded", DataType::Null, true));
                columns.push(arrow_array::new_null_array(&DataType::Null, 32768));
            }
            let source = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(source.clone(), columns).unwrap();
            let files = Files::new([
                ("present", FileData::new(target, vec![])),
                ("missing", FileData::new(source, vec![batch])),
            ]);
            let active = files.active.clone();
            let mut reader = FileSetAccessor::new(files)
                .with_union_by_name()
                .open(FileSetRequest::new(["present", "missing"]))
                .await
                .unwrap();
            let error = reader.next().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported, "{data_type}: {error}");
            assert!(error.to_string().contains("encoded length would overflow"));
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(active.load(Ordering::SeqCst), 0);
        }
    }
}

#[async_trait]
impl Accessor for Listing {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    fn capability(&self) -> Capability {
        Capability {
            list: true,
            read: false,
            read_with_range: false,
            read_parallel: false,
        }
    }
    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        unreachable!("discovery never reads bytes")
    }
}

#[async_trait]
impl ByteAccessor for Listing {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        if let Some(expected) = self.expected_prefix {
            assert_eq!(request.prefix.locator(), expected);
        }
        let mut prefix = request.prefix.locator().to_owned();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut items = self
            .objects
            .iter()
            .filter(|path| {
                request.recursive
                    || path
                        .strip_prefix(&prefix)
                        .is_none_or(|relative| !relative.contains('/'))
            })
            .map(|s| Ok(ObjectRef::new(*s)))
            .collect::<Vec<_>>();
        if self.fail {
            items.push(Err(Error::new(ErrorKind::Io, "list_fixture", "failed")));
        }
        Ok(Box::pin(stream::iter(items)))
    }
}

#[tokio::test]
async fn generic_listing_is_sorted_filtered_and_fails_before_opening_on_listing_error() {
    let schema = schema("same");
    let files = Files::new([
        (
            "data/.hidden.parquet",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[99])]),
        ),
        (
            "data/a.parquet",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[1])]),
        ),
        (
            "data/b.parquet",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[2])]),
        ),
    ]);
    let opens = files.opens.clone();
    let listing = Listing {
        expected_prefix: None,
        objects: vec![
            "data/b.parquet",
            "data/.hidden.parquet",
            "data/a.parquet",
            "data/b.parquet",
            "data/sub/c.parquet",
            "data/readme.txt",
        ],
        fail: false,
    };
    let accessor = DirectoryAccessor::new(listing, files.clone());
    let mut request = DirectoryRequest::new("data");
    request.recursive = false;
    request.suffix = Some(".parquet".into());
    assert_eq!(
        values(accessor.open(request).await.unwrap()).await,
        [99, 1, 2]
    );
    assert_eq!(opens.lock().unwrap().len(), 3);
    opens.lock().unwrap().clear();
    let accessor = DirectoryAccessor::new(
        Listing {
            expected_prefix: None,
            objects: vec!["data/a.parquet"],
            fail: true,
        },
        files.clone(),
    );
    assert_eq!(
        accessor
            .open(DirectoryRequest::new("data"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Io
    );
    assert!(opens.lock().unwrap().is_empty());
    let accessor = DirectoryAccessor::new(
        Listing {
            expected_prefix: None,
            objects: vec!["different/a.parquet"],
            fail: false,
        },
        files.clone(),
    );
    assert_eq!(
        accessor
            .open(DirectoryRequest::new("data"))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn directory_passes_backend_prefixes_and_locators_through_unchanged() {
    let schema = schema("same");
    for (prefix, path) in [
        ("data//sub//", "data//sub//a.parquet"),
        ("/data", "/data/a.parquet"),
        ("data/./sub", "data/./sub/a.parquet"),
        ("data/../sub", "data/../sub/a.parquet"),
        ("data", "data//a.parquet"),
        ("data", "data/./a.parquet"),
        ("data", "data/../a.parquet"),
        (
            "https://example.com/data//",
            "https://example.com/data//a.parquet",
        ),
    ] {
        let files = Files::new([(
            path,
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[1])]),
        )]);
        let opens = files.opens.clone();
        let accessor = DirectoryAccessor::new(
            Listing {
                expected_prefix: Some(prefix),
                objects: vec![path],
                fail: false,
            },
            files,
        );
        assert_eq!(
            values(accessor.open(DirectoryRequest::new(prefix)).await.unwrap()).await,
            [1]
        );
        assert_eq!(opens.lock().unwrap()[0].object.locator(), path);
    }
}

#[tokio::test]
async fn parallel_file_plan_is_lazy_and_preserves_options_order_and_duplicates() {
    let schema = schema("parallel");
    let files = Files::new([
        (
            "a",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[1, 2])]),
        ),
        (
            "b",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[3])]),
        ),
        ("empty", FileData::new(schema, vec![])),
    ]);
    let opens = files.opens.clone();
    let accessor = FileSetAccessor::new(files);
    let mut request = FileSetRequest::new(["b", "empty", "a", "b"]);
    request.options.batch_size = NonZeroUsize::new(2).unwrap();
    request.byte_options.io_concurrency = NonZeroUsize::new(3).unwrap();
    let plan = accessor.plan_scan(request.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint().0, 4);
    assert_eq!(opens.lock().unwrap().len(), 1);
    drop(plan);
    for p in [1, 3] {
        let parallel = accessor.clone().layer(ParallelReadLayer::new(
            NonZeroUsize::new(p).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ));
        assert_eq!(
            values(parallel.open(request.clone()).await.unwrap()).await,
            [3, 1, 2, 3]
        );
    }
    assert!(
        opens
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.options.batch_size.get() == 2 && r.byte_options.io_concurrency.get() == 3)
    );
    assert!(
        accessor
            .plan_scan(FileSetRequest::new(Vec::<ObjectRef>::new()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn parallel_directory_keeps_discovery_filter_sort_and_prefix_validation() {
    let schema = schema("parallel-directory");
    let files = Files::new([
        (
            "data/a",
            FileData::new(schema.clone(), vec![batch(schema.clone(), &[1])]),
        ),
        (
            "data/b",
            FileData::new(schema.clone(), vec![batch(schema, &[2])]),
        ),
    ]);
    let layer =
        ParallelReadLayer::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(1).unwrap());
    let directory = DirectoryAccessor::new(
        Listing {
            expected_prefix: None,
            objects: vec!["data/b", "data/a", "data/b", "data/sub/c"],
            fail: false,
        },
        files.clone(),
    )
    .layer(layer);
    let mut request = DirectoryRequest::new("data");
    request.recursive = false;
    assert_eq!(values(directory.open(request).await.unwrap()).await, [1, 2]);
    let calls = files.opens.lock().unwrap().len();
    let invalid = DirectoryAccessor::new(
        Listing {
            expected_prefix: None,
            objects: vec!["other/bad"],
            fail: false,
        },
        files.clone(),
    )
    .layer(layer);
    assert!(invalid.open(DirectoryRequest::new("data")).await.is_err());
    assert_eq!(files.opens.lock().unwrap().len(), calls);
}

#[derive(Clone)]
struct PlannedFiles {
    files: Files,
    plans: Arc<Mutex<Vec<ReadRequest>>>,
}

#[async_trait]
impl Accessor for PlannedFiles {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.files.capability()
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.files.open(request).await
    }
}

#[async_trait]
impl ScanPlanner for PlannedFiles {
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        self.plans.lock().unwrap().push(request.clone());
        let file = self
            .files
            .files
            .get(request.object.locator())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "plan_fixture", "missing"))?;
        let metadata = RecordMetadata {
            schema: file.schema.clone(),
            source: Some(request.object),
        };
        let partitions = file
            .batches
            .iter()
            .cloned()
            .map(|batch| {
                let files = self.files.clone();
                let metadata = metadata.clone();
                Box::pin(async move {
                    let active = files.active.fetch_add(1, Ordering::SeqCst) + 1;
                    files.max_active.fetch_max(active, Ordering::SeqCst);
                    Ok(Box::new(Batches {
                        active: files.active,
                        metadata,
                        batches: vec![batch].into_iter(),
                    }) as Box<dyn RecordReader>)
                }) as ScanPartition
            })
            .collect();
        Ok(ScanPlan::new(metadata, partitions)
            .with_ordering(open_arrow_access_core::OrderRequirement::Reorderable))
    }
}

#[tokio::test]
async fn native_file_plans_share_one_budget_and_preserve_lazy_ordered_selection() {
    let native_schema = schema("native");
    let files = Files::new([
        (
            "a",
            FileData::new(
                native_schema.clone(),
                vec![
                    batch(native_schema.clone(), &[1, 2]),
                    batch(native_schema.clone(), &[3]),
                ],
            ),
        ),
        (
            "b",
            FileData::new(
                native_schema.clone(),
                vec![
                    batch(native_schema.clone(), &[4]),
                    batch(native_schema.clone(), &[5]),
                ],
            ),
        ),
        ("empty", FileData::new(native_schema, vec![])),
        ("mismatch", FileData::new(schema("different"), vec![])),
    ]);
    let plans = Arc::new(Mutex::new(Vec::new()));
    let accessor = FileSetAccessor::new(PlannedFiles {
        files: files.clone(),
        plans: plans.clone(),
    })
    .with_file_planning();
    let mut request = FileSetRequest::new(["b", "empty", "a", "b"]);
    request.options.batch_size = NonZeroUsize::new(2).unwrap();
    request.byte_options.io_concurrency = NonZeroUsize::new(3).unwrap();
    let plan = accessor.plan_scan(request.clone()).await.unwrap();
    assert_eq!(
        plan.ordering,
        open_arrow_access_core::OrderRequirement::PreserveGlobal
    );
    assert_eq!(plans.lock().unwrap().len(), 1);
    assert_eq!(files.active.load(Ordering::SeqCst), 0);
    assert!(files.opens.lock().unwrap().is_empty());
    drop(plan);
    plans.lock().unwrap().clear();
    let parallel = accessor.clone().layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    assert_eq!(
        values(parallel.open(request).await.unwrap()).await,
        [4, 5, 1, 2, 3, 4, 5]
    );
    assert_eq!(files.active.load(Ordering::SeqCst), 0);
    assert!(files.max_active.load(Ordering::SeqCst) <= 2);
    assert!(files.opens.lock().unwrap().is_empty());
    {
        let observed = plans.lock().unwrap();
        assert_eq!(
            observed
                .iter()
                .map(|r| r.object.locator())
                .collect::<Vec<_>>(),
            ["b", "empty", "a", "b"]
        );
        assert!(
            observed.iter().all(
                |r| r.options.batch_size.get() == 2 && r.byte_options.io_concurrency.get() == 3
            )
        );
    }
    let nested = FileSetAccessor::new(
        PlannedFiles {
            files: files.clone(),
            plans: plans.clone(),
        }
        .layer(ParallelReadLayer::new(NonZeroUsize::MIN, NonZeroUsize::MIN)),
    )
    .with_file_planning();
    let error = nested
        .plan_scan(FileSetRequest::new(["a"]))
        .await
        .err()
        .unwrap();
    assert_eq!(error.operation(), "plan_file_set");
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    let mut plan = accessor
        .plan_scan(FileSetRequest::new(["empty", "mismatch", "a"]))
        .await
        .unwrap();
    use futures::StreamExt;
    let error = plan.partitions.next().await.unwrap().err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), "read_file_set");
    assert_eq!(
        plans.lock().unwrap().last().unwrap().object.locator(),
        "mismatch"
    );
}

#[tokio::test]
async fn union_by_name_preflight_rejects_type_and_metadata_conflicts_and_drops_readers() {
    for (left, right) in [
        (
            schema("same"),
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        ),
        (schema("same"), schema("different")),
        (
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false)
                    .with_metadata([("key".to_owned(), "left".to_owned())].into()),
            ])),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false)
                    .with_metadata([("key".to_owned(), "right".to_owned())].into()),
            ])),
        ),
    ] {
        let files = Files::new([
            ("a", FileData::new(left, vec![])),
            ("b", FileData::new(right, vec![])),
        ]);
        let accessor = FileSetAccessor::new(files.clone()).with_union_by_name();
        for projection in [None, Some(vec![])] {
            let error = accessor
                .open(FileSetRequest::new(["a", "b"]).with_options(ReadOptions {
                    projection,
                    ..Default::default()
                }))
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert_eq!(files.max_active.load(Ordering::SeqCst), 1);
            assert_eq!(files.active.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn union_by_name_still_checks_each_batch_against_its_source_schema() {
    let declared = schema("declared");
    let other = schema("other");
    let files = Files::new([("a", FileData::new(declared, vec![batch(other, &[1])]))]);
    let accessor = FileSetAccessor::new(files.clone()).with_union_by_name();
    let mut reader = accessor.open(FileSetRequest::new(["a"])).await.unwrap();
    assert_eq!(files.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        reader.next().await.unwrap_err().operation(),
        "read_file_set"
    );
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(files.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn union_by_name_rejects_a_schema_that_changes_after_preflight() {
    #[derive(Clone)]
    struct Changing(Arc<AtomicUsize>);
    #[async_trait]
    impl Accessor for Changing {
        type Request = ReadRequest;
        type Reader = dyn RecordReader;
        fn capability(&self) -> RecordCapability {
            RecordCapability::sequential(OrderRequirement::PreserveGlobal)
        }
        async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
            let label = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                "before"
            } else {
                "after"
            };
            Ok(Box::new(open_arrow_access_core::StreamReader::records(
                stream::empty(),
                RecordMetadata {
                    schema: schema(label),
                    source: Some(request.object),
                },
                OrderRequirement::PreserveGlobal,
            )))
        }
    }
    for projection in [None, Some(vec!["id".into()])] {
        let accessor = FileSetAccessor::new(Changing(Arc::default())).with_union_by_name();
        let mut reader = accessor
            .open(FileSetRequest::new(["a"]).with_options(ReadOptions {
                projection,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap_err().operation(),
            "read_file_set"
        );
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn glob_selection_respects_paths_recursion_and_existing_suffix_filter() {
    let schema = schema("same");
    let names = [
        ("data/a.parquet", 1),
        ("data/b.parquet", 2),
        ("data/topic-x/train.parquet", 3),
        ("data/topic-x/deep/train.parquet", 4),
        ("data/topic-y/test.json", 5),
        ("data/.hidden.parquet", 6),
    ];
    for (pattern, recursive, suffix, expected) in [
        ("*.parquet", true, None, vec![6, 1, 2]),
        ("topic*/**/*.parquet", true, None, vec![4, 3]),
        ("topic*/**/*", true, Some(".parquet"), vec![4, 3]),
        ("**/*.parquet", false, None, vec![6, 1, 2]),
    ] {
        let files = Files::new(names.map(|(name, id)| {
            (
                name,
                FileData::new(schema.clone(), vec![batch(schema.clone(), &[id])]),
            )
        }));
        let accessor = DirectoryAccessor::new(
            Listing {
                expected_prefix: Some("data/"),
                objects: names
                    .iter()
                    .map(|(name, _)| *name)
                    .chain(["data/b.parquet"])
                    .collect(),
                fail: false,
            },
            files,
        );
        let mut request = DirectoryRequest::new(format!("data/{pattern}"));
        request.recursive = recursive;
        request.suffix = suffix.map(str::to_owned);
        let reader = accessor.open(request).await.unwrap();
        assert_eq!(values(reader).await, expected, "{pattern}");
    }
}
