use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use more_asserts::assert_gt;
use open_arrow_access::{
    Accessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error, ErrorKind,
    Reader, Result, async_trait,
};

#[derive(Default)]
pub struct Observations {
    pub requests: Mutex<Vec<ByteReadRequest>>,
    pub reads: AtomicUsize,
}

#[derive(Clone)]
pub struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    pub fail_after: Option<usize>,
    pub observations: Arc<Observations>,
}

impl MemoryAccessor {
    pub fn new(data: &[u8], chunk_size: usize) -> Self {
        assert_gt!(chunk_size, 0);
        Self {
            data: Bytes::copy_from_slice(data),
            chunk_size,
            fail_after: None,
            observations: Arc::default(),
        }
    }
}

#[async_trait]
impl Accessor for MemoryAccessor {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
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
            offset: 0,
            fail_after: self.fail_after,
            observations: self.observations.clone(),
            metadata: ByteMetadata {
                source: Some(request.object),
                ..ByteMetadata::default()
            },
        }))
    }
}

struct MemoryReader {
    data: Bytes,
    chunk_size: usize,
    offset: usize,
    fail_after: Option<usize>,
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
            read_with_range: false,
            list: false,
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        if self.fail_after.is_some_and(|offset| self.offset >= offset) {
            return Err(Error::new(ErrorKind::Io, "memory_read", "injected failure"));
        }
        if self.offset == self.data.len() {
            return Ok(None);
        }
        let end = (self.offset + self.chunk_size).min(self.data.len());
        let bytes = self.data.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}

#[async_trait]
impl ByteReader for MemoryReader {}
