use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes,
    Capability, Error, ErrorKind, Layer, ObjectRef, ObjectStream, Reader, RecordBatch, Result,
    SchemaRef, async_trait,
};
use parquet::basic::Compression;

pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub schema: SchemaRef,
}

pub fn fixture(compression: Compression) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("title", DataType::Utf8, true),
        ],
        [("fixture".into(), "ordered".into())].into(),
    ));
    let name = match compression {
        Compression::UNCOMPRESSED => "ordered-uncompressed.parquet",
        Compression::SNAPPY => "ordered-snappy.parquet",
        other => panic!("no checked-in Parquet fixture for {other:?}"),
    };
    std::fs::copy(fixture_path(name), dir.path().join("data.parquet")).unwrap();
    Fixture { dir, schema }
}

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/parquet")
        .join(name)
}

pub fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

pub use open_arrow_access_test_support::MemoryAccessor;

#[derive(Default)]
pub struct Observations {
    pub byte_options: Mutex<Vec<ByteReadOptions>>,
    pub batch_reads: AtomicUsize,
    pub max_batch_ranges: AtomicUsize,
    pub range_reads: AtomicUsize,
    pub fail_reads: AtomicBool,
    pub failure_kind: Mutex<Option<ErrorKind>>,
    pub drops: AtomicUsize,
}

/// A test-only layer validating range delegation and failure/cancellation behavior.
pub struct ByteObserverLayer(pub Arc<Observations>);

pub struct ByteObserverAccessor<A> {
    inner: A,
    observations: Arc<Observations>,
}

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Layer<A>
    for ByteObserverLayer
{
    type Layered = ByteObserverAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        ByteObserverAccessor {
            inner,
            observations: self.0.clone(),
        }
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor
    for ByteObserverAccessor<A>
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            list: inner.list,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.observations
            .byte_options
            .lock()
            .unwrap()
            .push(request.options);
        Ok(Box::new(ByteObserverReader {
            inner: self.inner.open(request).await?,
            observations: self.observations.clone(),
        }) as Box<dyn ByteReader>)
    }
}

struct ByteObserverReader {
    inner: Box<dyn ByteReader>,
    observations: Arc<Observations>,
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for ByteObserverAccessor<A> {
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        self.inner.list(prefix).await
    }
}

impl Drop for ByteObserverReader {
    fn drop(&mut self) {
        self.observations.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for ByteObserverReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: inner.read_with_range,
            list: inner.list,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        self.inner.metadata()
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.next().await
    }
}

#[async_trait]
impl ByteReader for ByteObserverReader {
    async fn read_ranges(&mut self, ranges: Vec<Range<u64>>) -> Result<Vec<Bytes>> {
        self.observations.batch_reads.fetch_add(1, Ordering::SeqCst);
        self.observations
            .max_batch_ranges
            .fetch_max(ranges.len(), Ordering::SeqCst);
        self.observations
            .range_reads
            .fetch_add(ranges.len(), Ordering::SeqCst);
        if self.observations.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::new(
                self.observations
                    .failure_kind
                    .lock()
                    .unwrap()
                    .unwrap_or(ErrorKind::Io),
                "read_ranges",
                "injected read failure",
            )
            .with_source(std::io::Error::other("injected source failure")));
        }
        self.inner.read_ranges(ranges).await
    }

    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.observations.range_reads.fetch_add(1, Ordering::SeqCst);
        if self.observations.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::new(
                self.observations
                    .failure_kind
                    .lock()
                    .unwrap()
                    .unwrap_or(ErrorKind::Io),
                "read_range",
                "injected read failure",
            )
            .with_source(std::io::Error::other("injected source failure")));
        }
        self.inner.read_range(range).await
    }
}

/// Test-only wrapper modeling a decoder that cannot preserve upstream byte ranges.
pub struct SequentialLayer;

pub struct SequentialAccessor<A>(A);

impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Layer<A> for SequentialLayer {
    type Layered = SequentialAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        SequentialAccessor(inner)
    }
}

#[async_trait]
impl<A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>> Accessor
    for SequentialAccessor<A>
{
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            read_with_range: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(SequentialReader(self.0.open(request).await?)) as Box<dyn ByteReader>)
    }
}

struct SequentialReader(Box<dyn ByteReader>);

#[async_trait]
impl Reader for SequentialReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            read_with_range: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        self.0.metadata()
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.0.next().await
    }
}

#[async_trait]
impl ByteReader for SequentialReader {}
