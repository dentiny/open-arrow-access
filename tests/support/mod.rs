use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access::{
    Accessor, ByteAccessor, ByteMetadata, ByteReader, Bytes, Capability, Error, ErrorKind, Layer,
    ObjectRef, ObjectStream, Reader, RecordBatch, Result, SchemaRef, async_trait,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

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
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..7)),
        Arc::new(StringArray::from(vec![
            Some("zero"),
            None,
            Some("two"),
            Some("three"),
            None,
            Some("five"),
            Some("six"),
        ])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_compression(compression)
        .build();
    let file = std::fs::File::create(dir.path().join("data.parquet")).unwrap();
    // Writer use is limited to generating test fixtures, not part of the library API.
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Fixture { dir, schema }
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

/// An application-owned test backend; no dependency on a production storage crate.
pub struct MemoryAccessor {
    bytes: Bytes,
    ranged: bool,
}

impl MemoryAccessor {
    pub fn new(bytes: Bytes, ranged: bool) -> Self {
        Self { bytes, ranged }
    }
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    /// Report sequential reads and the range support configured for this fixture.
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: self.ranged,
            ..Capability::default()
        }
    }

    /// Return an application-owned in-memory reader for the requested opaque locator.
    async fn open(&self, source: ObjectRef) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(MemoryReader {
            metadata: ByteMetadata {
                source: Some(source),
                content_length: Some(self.bytes.len() as u64),
                version: None,
            },
            bytes: self.bytes.clone(),
            ranged: self.ranged,
            emitted: false,
        }) as Box<dyn ByteReader>)
    }
}

struct MemoryReader {
    bytes: Bytes,
    metadata: ByteMetadata,
    ranged: bool,
    emitted: bool,
}

#[async_trait]
impl Reader for MemoryReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    /// Report sequential reads and the range support configured for this fixture.
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: self.ranged,
            ..Capability::default()
        }
    }

    /// Describe the in-memory byte representation exposed by this fixture reader.
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    /// Emit the fixture bytes once, then report EOF without producing empty chunks.
    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.emitted || self.bytes.is_empty() {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(self.bytes.clone()))
    }
}

#[async_trait]
impl ByteReader for MemoryReader {
    /// Return the exact requested slice or reject unsupported/invalid ranges.
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        if !self.ranged {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "read_range",
                "sequential only",
            ));
        }
        if range.start > range.end || range.end > self.bytes.len() as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "read_range",
                "invalid range",
            ));
        }
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
}

#[derive(Default)]
pub struct Observations {
    pub range_reads: AtomicUsize,
    pub fail_reads: AtomicBool,
    pub drops: AtomicUsize,
}

/// A test-only layer validating range delegation and failure/cancellation behavior.
pub struct ByteObserverLayer(pub Arc<Observations>);

pub struct ByteObserverAccessor<A> {
    inner: A,
    observations: Arc<Observations>,
}

impl<A: Accessor<Request = ObjectRef, Reader = dyn ByteReader>> Layer<A> for ByteObserverLayer {
    type Layered = ByteObserverAccessor<A>;

    /// Attach shared observations while preserving the byte accessor contract.
    fn layer(&self, inner: A) -> Self::Layered {
        ByteObserverAccessor {
            inner,
            observations: self.0.clone(),
        }
    }
}

#[async_trait]
impl<A: Accessor<Request = ObjectRef, Reader = dyn ByteReader>> Accessor
    for ByteObserverAccessor<A>
{
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    /// Forward effective capabilities because observation preserves supported operations.
    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    /// Open the inner input and wrap its reader with range/drop observations.
    async fn open(&self, request: ObjectRef) -> Result<Box<dyn ByteReader>> {
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
    /// Preserve discovery whenever the observed byte accessor exposes it.
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        self.inner.list(prefix).await
    }
}

impl Drop for ByteObserverReader {
    /// Record reader destruction to verify that stopping consumption releases input.
    fn drop(&mut self) {
        self.observations.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Reader for ByteObserverReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    /// Forward effective capabilities because observation preserves supported operations.
    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    /// Forward metadata without altering the input representation.
    fn metadata(&self) -> &ByteMetadata {
        self.inner.metadata()
    }

    /// Delegate sequential consumption without changing chunks, EOF or errors.
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.next().await
    }
}

#[async_trait]
impl ByteReader for ByteObserverReader {
    /// Count each range request, inject a requested failure, or delegate unchanged.
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.observations.range_reads.fetch_add(1, Ordering::SeqCst);
        if self.observations.fail_reads.load(Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Io,
                "read_range",
                "injected read failure",
            ));
        }
        self.inner.read_range(range).await
    }
}

/// Test-only wrapper modeling a decoder that cannot preserve upstream byte ranges.
pub struct SequentialLayer;

pub struct SequentialAccessor<A>(A);

impl<A: Accessor<Request = ObjectRef, Reader = dyn ByteReader>> Layer<A> for SequentialLayer {
    type Layered = SequentialAccessor<A>;

    /// Wrap a byte accessor while preserving its request and reader types.
    fn layer(&self, inner: A) -> Self::Layered {
        SequentialAccessor(inner)
    }
}

#[async_trait]
impl<A: Accessor<Request = ObjectRef, Reader = dyn ByteReader>> Accessor for SequentialAccessor<A> {
    type Request = ObjectRef;
    type Reader = dyn ByteReader;

    /// Remove range access and discovery, which this wrapper does not implement.
    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            ..Capability::default()
        }
    }

    /// Open the wrapped input behind a sequential-only reader interface.
    async fn open(&self, request: ObjectRef) -> Result<Box<dyn ByteReader>> {
        Ok(Box::new(SequentialReader(self.0.open(request).await?)) as Box<dyn ByteReader>)
    }
}

struct SequentialReader(Box<dyn ByteReader>);

#[async_trait]
impl Reader for SequentialReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    /// Advertise only the sequential consumption exposed by this wrapper.
    fn capability(&self) -> Capability {
        Capability {
            read: self.0.capability().read,
            ..Capability::default()
        }
    }

    /// Preserve metadata because this test wrapper does not transform the bytes.
    fn metadata(&self) -> &ByteMetadata {
        self.0.metadata()
    }

    /// Forward sequential chunks without exposing upstream range access.
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.0.next().await
    }
}

#[async_trait]
impl ByteReader for SequentialReader {}
