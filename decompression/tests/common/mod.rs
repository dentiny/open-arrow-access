use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadRequest, ByteReader, Bytes, Capability, Error,
    ErrorKind, ObjectRef, ObjectStream, Reader, Result, async_trait,
};

#[derive(Clone)]
pub struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    pub error: Option<ErrorKind>,
    pub polls: Arc<AtomicUsize>,
    pub requests: Arc<Mutex<Vec<ByteReadRequest>>>,
}

impl MemoryAccessor {
    pub fn new(bytes: &[u8], chunk_size: usize) -> Self {
        Self {
            data: Bytes::copy_from_slice(bytes),
            chunk_size,
            error: None,
            polls: Arc::default(),
            requests: Arc::default(),
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
            read_with_range: true,
            list: true,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(Box::new(MemoryReader {
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.data.len() as u64),
                version: Some("source-version".into()),
            },
            data: self.data.clone(),
            chunk_size: self.chunk_size,
            offset: 0,
            finished: false,
            error: self.error,
            polls: self.polls.clone(),
        }))
    }
}

#[async_trait]
impl ByteAccessor for MemoryAccessor {
    async fn list(&self, prefix: ObjectRef) -> Result<ObjectStream> {
        Ok(Box::pin(futures::stream::iter([Ok(prefix)])))
    }
}

struct MemoryReader {
    metadata: ByteMetadata,
    data: Bytes,
    chunk_size: usize,
    offset: usize,
    finished: bool,
    error: Option<ErrorKind>,
    polls: Arc<AtomicUsize>,
}

#[async_trait]
impl Reader for MemoryReader {
    type Item = Bytes;
    type Metadata = ByteMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: true,
            ..Capability::default()
        }
    }

    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<Bytes>> {
        if self.finished {
            return Ok(None);
        }
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.offset < self.data.len() {
            let end = (self.offset + self.chunk_size).min(self.data.len());
            let chunk = self.data.slice(self.offset..end);
            self.offset = end;
            return Ok(Some(chunk));
        }
        self.finished = true;
        if let Some(kind) = self.error.take() {
            return Err(Error::new(kind, "upstream", "injected source failure"));
        }
        Ok(None)
    }
}

#[async_trait]
impl ByteReader for MemoryReader {
    async fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.data.len() as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "memory_range",
                "invalid range",
            ));
        }
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

pub async fn collect(reader: &mut dyn ByteReader) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(chunk) = reader.next().await? {
        assert!(!chunk.is_empty());
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}
