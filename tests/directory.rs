use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType, Field};
use futures::{TryStreamExt, stream};
use open_arrow_access::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ObjectRef,
    ObjectStream, ReadOptionsProvider, ReadRequest, Reader, RecordBatch, RecordMetadata,
    RecordReader, Result, Schema, SchemaRef, async_trait,
    dataset::{DirectoryAccessor, DirectoryRequest, FileSetAccessor, FileSetRequest},
    into_stream,
};

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
struct Files {
    files: HashMap<String, (SchemaRef, Vec<RecordBatch>)>,
    opens: Arc<Mutex<Vec<ReadRequest>>>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl Files {
    fn new(files: impl IntoIterator<Item = (&'static str, SchemaRef, Vec<RecordBatch>)>) -> Self {
        Self {
            files: files
                .into_iter()
                .map(|(name, schema, batches)| (name.into(), (schema, batches)))
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

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        self.opens.lock().unwrap().push(request.clone());
        let (schema, batches) = self
            .files
            .get(request.object.locator())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "open_fixture", "missing"))?;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        Ok(Box::new(Batches {
            active: self.active.clone(),
            metadata: RecordMetadata {
                schema: schema.clone(),
                source: Some(request.object),
            },
            batches: batches.clone().into_iter(),
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
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
        }
    }
    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

async fn values(reader: Box<dyn RecordReader>) -> Vec<i64> {
    into_stream(reader)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn file_set_keeps_explicit_order_duplicates_and_opens_lazily() {
    let schema = schema("same");
    let files = Files::new([
        ("a", schema.clone(), vec![batch(schema.clone(), &[1, 2])]),
        ("b", schema.clone(), vec![batch(schema.clone(), &[3])]),
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
        ("empty", schema.clone(), vec![]),
        ("data", schema.clone(), vec![batch(schema.clone(), &[7])]),
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
        ("first", first.clone(), vec![batch(first.clone(), &[1])]),
        ("mismatch", other, vec![]),
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
    let files = Files::new([("bad", declared, vec![batch(other, &[1])])]);
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
    objects: Vec<&'static str>,
    fail: bool,
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
        }
    }
    async fn open(&self, _: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        unreachable!("discovery never reads bytes")
    }
}

#[async_trait]
impl ByteAccessor for Listing {
    async fn list(&self, _: ObjectRef) -> Result<ObjectStream> {
        let mut items = self
            .objects
            .iter()
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
            "data/a.parquet",
            schema.clone(),
            vec![batch(schema.clone(), &[1])],
        ),
        (
            "data/b.parquet",
            schema.clone(),
            vec![batch(schema.clone(), &[2])],
        ),
    ]);
    let opens = files.opens.clone();
    let listing = Listing {
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
    assert_eq!(values(accessor.open(request).await.unwrap()).await, [1, 2]);
    assert_eq!(opens.lock().unwrap().len(), 2);
    opens.lock().unwrap().clear();
    let accessor = DirectoryAccessor::new(
        Listing {
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
async fn directory_rejects_traversal_entries_even_when_hidden_files_are_enabled() {
    let files = Files::new([]);
    for path in ["data/../outside", "data/./a", "data//absolute"] {
        let accessor = DirectoryAccessor::new(
            Listing {
                objects: vec![path],
                fail: false,
            },
            files.clone(),
        );
        let mut request = DirectoryRequest::new("data");
        request.include_hidden = true;
        assert_eq!(
            accessor.open(request).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
        assert!(files.opens.lock().unwrap().is_empty());
    }
}

#[cfg(all(feature = "storage-fs", feature = "format-parquet"))]
mod filesystem {
    use super::*;
    use more_asserts::assert_le;
    use open_arrow_access::{file_format::ParquetAccessor, storage::FsAccessor};
    fn copy_fixture(name: &str, destination: &std::path::Path) {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/parquet/directory")
            .join(name);
        std::fs::copy(source, destination).unwrap();
    }

    #[tokio::test]
    async fn filesystem_listing_is_recursive_file_only_and_validates_directory_paths() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("a"), b"a").unwrap();
        std::fs::write(dir.path().join("sub/b"), b"b").unwrap();
        let fs = FsAccessor::new(dir.path()).await.unwrap();
        assert!(fs.capability().list);
        let mut paths = fs
            .list("".into())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        paths.sort_by(|a, b| a.locator().cmp(b.locator()));
        assert_eq!(paths, vec![ObjectRef::new("a"), ObjectRef::new("sub/b")]);
        assert_eq!(
            fs.list("sub".into())
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![ObjectRef::new("sub/b")]
        );
        for path in ["../outside", "/absolute", ".", " sub"] {
            assert_eq!(
                fs.list(path.into()).await.err().unwrap().kind(),
                ErrorKind::InvalidInput
            );
        }
        assert_eq!(
            fs.list("missing".into()).await.err().unwrap().kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            fs.list("a".into()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn directory_reads_local_parquet_in_sorted_order_with_projection_and_byte_options() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let schema = schema("same");
        copy_fixture("b.parquet", &dir.path().join("b.parquet"));
        copy_fixture("a.parquet", &dir.path().join("a.parquet"));
        copy_fixture("c.parquet", &dir.path().join("sub/c.parquet"));
        copy_fixture("hidden.parquet", &dir.path().join(".hidden.parquet"));
        std::fs::write(dir.path().join("readme"), b"not parquet").unwrap();
        let fs = FsAccessor::new(dir.path()).await.unwrap();
        let accessor = DirectoryAccessor::new(fs.clone(), ParquetAccessor::new(fs));
        let mut request = DirectoryRequest::new("");
        request.suffix = Some(".parquet".into());
        request.options.batch_size = NonZeroUsize::new(1).unwrap();
        request.byte_options.io_concurrency = NonZeroUsize::new(2).unwrap();
        request.byte_options.io_chunk_size = NonZeroUsize::new(8);
        let reader = accessor.open(request.clone()).await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        assert_eq!(values(reader).await, [1, 2, 3, 4]);
        request.options.projection = Some(vec![]);
        let batches = into_stream(accessor.open(request.clone()).await.unwrap())
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 0);
            assert_le!(batch.num_rows(), 1);
        }
        request.include_hidden = true;
        request.recursive = false;
        request.options.projection = None;
        assert_eq!(
            values(accessor.open(request.clone()).await.unwrap()).await,
            [99, 1, 2, 3]
        );
        request.suffix = Some(".csv".into());
        assert_eq!(
            accessor.open(request).await.err().unwrap().kind(),
            ErrorKind::InvalidInput
        );
    }
}
