//! Shared byte fixtures and operator setup for integration tests.
//! Depend on this crate only through dev-dependencies.

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use more_asserts::assert_gt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ListRequest, ObjectStream, Reader, Result, async_trait,
};

/// Configure the filesystem SDK backend for test fixtures.
pub fn fs_operator(root: impl AsRef<Path>) -> opendal::Operator {
    opendal::Operator::new(opendal::services::Fs::default().root(root.as_ref().to_str().unwrap()))
        .unwrap()
}

/// Calls observed across accessor clones and all readers opened from them.
#[derive(Default)]
pub struct Observations {
    pub requests: Mutex<Vec<ByteReadRequest>>,
    /// Counts every `next` call, including calls after EOF or failure.
    pub reads: AtomicUsize,
    pub range_reads: AtomicUsize,
    pub ranges: Mutex<Vec<Range<u64>>>,
}

#[derive(Clone, Copy)]
enum Fault {
    Failure { offset: usize, kind: ErrorKind },
    EmptyChunk { offset: usize },
}

/// An in-memory byte source with independent reader cursors and shared observations.
/// Defaults to sequential reads, known content length, no version, and no listing.
#[derive(Clone)]
pub struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    ranged: bool,
    listing: bool,
    version: Option<String>,
    fault: Option<Fault>,
    open_failure: Option<ErrorKind>,
    range_failure: Option<(usize, ErrorKind)>,
    short_ranges: bool,
    known_length: bool,
    pub observations: Arc<Observations>,
}

impl MemoryAccessor {
    pub fn new(data: &[u8], chunk_size: usize) -> Self {
        assert_gt!(chunk_size, 0);
        Self {
            data: Bytes::copy_from_slice(data),
            chunk_size,
            ranged: false,
            listing: false,
            version: None,
            fault: None,
            open_failure: None,
            range_failure: None,
            short_ranges: false,
            known_length: true,
            observations: Arc::default(),
        }
    }

    pub fn with_ranges(mut self) -> Self {
        self.ranged = true;
        self
    }

    /// Fail range reads after this many successful calls, independently per reader.
    pub fn with_range_failure(mut self, after: usize, kind: ErrorKind) -> Self {
        self.range_failure = Some((after, kind));
        self
    }

    /// Return one byte less than requested to exercise exact-read validation.
    pub fn with_short_ranges(mut self) -> Self {
        self.short_ranges = true;
        self
    }

    pub fn without_content_length(mut self) -> Self {
        self.known_length = false;
        self
    }

    /// Enable a minimal listing that returns the requested prefix as its only object.
    pub fn with_listing(mut self) -> Self {
        self.listing = true;
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Fail every open after recording its request, without creating or polling a reader.
    pub fn with_open_failure(mut self, kind: ErrorKind) -> Self {
        self.open_failure = Some(kind);
        self
    }

    /// Fail the next sequential read at or beyond this byte offset, including EOF.
    /// Each opened reader injects its own failure and then terminates.
    pub fn with_failure(mut self, offset: usize, kind: ErrorKind) -> Self {
        self.fault = Some(Fault::Failure { offset, kind });
        self
    }

    /// Deliberately violate the nonempty-chunk contract once, at or beyond `offset`.
    /// Use this to verify a consumer rejects malformed upstream readers.
    pub fn with_empty_chunk(mut self, offset: usize) -> Self {
        self.fault = Some(Fault::EmptyChunk { offset });
        self
    }
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: self.ranged,
            read_parallel: false,
            list: self.listing,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.observations
            .requests
            .lock()
            .unwrap()
            .push(request.clone());
        if let Some(kind) = self.open_failure {
            return Err(
                Error::new(kind, "memory_open", "injected open failure").with_source(
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected open failure",
                    ),
                ),
            );
        }
        Ok(Box::new(MemoryReader {
            data: self.data.clone(),
            chunk_size: self.chunk_size,
            ranged: self.ranged,
            offset: 0,
            finished: false,
            fault: self.fault,
            range_failure: self.range_failure,
            range_calls: 0,
            short_ranges: self.short_ranges,
            observations: self.observations.clone(),
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: self.known_length.then_some(self.data.len() as u64),
                version: self.version.clone(),
            },
        }))
    }
}

#[async_trait]
impl ByteAccessor for MemoryAccessor {
    async fn list(&self, request: ListRequest) -> Result<ObjectStream> {
        if !self.listing {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "list",
                "listing is disabled",
            ));
        }
        Ok(Box::pin(futures::stream::iter([Ok(request.prefix)])))
    }
}

struct MemoryReader {
    data: Bytes,
    chunk_size: usize,
    ranged: bool,
    offset: usize,
    finished: bool,
    fault: Option<Fault>,
    range_failure: Option<(usize, ErrorKind)>,
    short_ranges: bool,
    range_calls: usize,
    observations: Arc<Observations>,
    metadata: ByteMetadata,
}

#[async_trait]
impl Reader for MemoryReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: self.ranged,
            read_parallel: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        if self.finished {
            return Ok(None);
        }
        match self.fault {
            Some(Fault::Failure { offset, kind }) if self.offset >= offset => {
                self.finished = true;
                return Err(Error::new(kind, "memory_read", "injected source failure")
                    .with_source(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected source failure",
                    )));
            }
            Some(Fault::EmptyChunk { offset }) if self.offset >= offset => {
                self.fault = None;
                return Ok(Some(Bytes::new()));
            }
            _ => {}
        }
        if self.offset == self.data.len() {
            self.finished = true;
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.chunk_size)
            .min(self.data.len());
        let bytes = self.data.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}

#[async_trait]
impl ByteReader for MemoryReader {
    async fn read_range(&mut self, range: Range<u64>) -> Result<Bytes> {
        self.observations.range_reads.fetch_add(1, Ordering::SeqCst);
        self.observations.ranges.lock().unwrap().push(range.clone());
        if !self.ranged {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "read_range",
                "sequential only",
            ));
        }
        if range.start > range.end || range.end > self.data.len() as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "read_range",
                "invalid range",
            ));
        }
        let call = self.range_calls;
        self.range_calls += 1;
        if let Some((after, kind)) = self.range_failure
            && call >= after
        {
            return Err(
                Error::new(kind, "memory_range", "injected range failure").with_source(
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "injected range failure",
                    ),
                ),
            );
        }
        let mut bytes = self.data.slice(range.start as usize..range.end as usize);
        if self.short_ranges {
            bytes.truncate(bytes.len().saturating_sub(1));
        }
        Ok(bytes)
    }
}

/// Shared assertions for sequential byte layers.
pub mod byte_layer;

pub mod byte_accessor;

/// Shared assertions for single-file Arrow format accessors.
pub mod format;

/// Shared helpers for record-reader values.
pub mod record;

fn memory_read_error(error: &Error) -> &Error {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(shared) = source.downcast_ref::<Error>()
            && shared.operation() == "memory_read"
        {
            return shared;
        }
        source = match source
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            Some(inner) => inner,
            None => source.source().expect("missing original upstream error"),
        };
    }
}

/// Resolve a committed fixture from the calling crate's workspace.
pub fn fixture_path(manifest_dir: impl AsRef<Path>, format: &str, name: &str) -> PathBuf {
    let manifest_dir = manifest_dir.as_ref();
    let root = manifest_dir
        .ancestors()
        .find(|root| root.join("testdata").is_dir())
        .unwrap_or_else(|| panic!("no testdata directory above {}", manifest_dir.display()));
    root.join("testdata").join(format).join(name)
}

/// Read a committed fixture, reporting its full path if reading fails.
pub async fn read_fixture(manifest_dir: impl AsRef<Path>, format: &str, name: &str) -> Vec<u8> {
    let path = fixture_path(manifest_dir, format, name);
    tokio::fs::read(&path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// Locate `testdata/<format>/<name>` from the calling crate's workspace.
#[macro_export]
macro_rules! fixture_path {
    ($format:expr, $name:expr) => {
        $crate::fixture_path(env!("CARGO_MANIFEST_DIR"), $format, $name)
    };
}

/// Read `testdata/<format>/<name>` from the calling crate's workspace.
/// Resolve the manifest directory at the call site so shared build caches cannot
/// bind fixtures to a different checkout's test-support compilation directory.
#[macro_export]
macro_rules! fixture {
    ($format:expr, $name:expr) => {
        $crate::read_fixture(env!("CARGO_MANIFEST_DIR"), $format, $name)
    };
}

/// Collect a byte reader, asserting its nonempty-chunk contract.
pub async fn collect_bytes(reader: &mut dyn ByteReader) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        assert!(!bytes.is_empty());
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}

/// HTTP route fixtures with isolated hosts and request observations.
pub mod http;
