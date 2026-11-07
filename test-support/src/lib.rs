//! Shared byte backend for integration tests. Depend on this crate only through dev-dependencies.

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

/// Calls observed across accessor clones and all readers opened from them.
#[derive(Default)]
pub struct Observations {
    pub requests: Mutex<Vec<ByteReadRequest>>,
    /// Counts every `next` call, including calls after EOF or failure.
    pub reads: AtomicUsize,
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
            observations: Arc::default(),
        }
    }

    pub fn with_ranges(mut self) -> Self {
        self.ranged = true;
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
        Ok(Box::new(MemoryReader {
            data: self.data.clone(),
            chunk_size: self.chunk_size,
            ranged: self.ranged,
            offset: 0,
            finished: false,
            fault: self.fault,
            observations: self.observations.clone(),
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.data.len() as u64),
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
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

/// Shared assertions for sequential byte layers.
pub mod byte_layer;

/// Resolve a committed fixture relative to the calling crate's manifest directory.
pub fn fixture_path(manifest_dir: impl AsRef<Path>, format: &str, name: &str) -> PathBuf {
    manifest_dir
        .as_ref()
        .join("../testdata")
        .join(format)
        .join(name)
}

/// Read a committed fixture, reporting its full path if reading fails.
pub async fn read_fixture(manifest_dir: impl AsRef<Path>, format: &str, name: &str) -> Vec<u8> {
    let path = fixture_path(manifest_dir, format, name);
    tokio::fs::read(&path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
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
