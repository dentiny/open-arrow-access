pub fn fixture(format: &str, name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("testdata")
        .join(format)
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest, ByteReader, Bytes,
    Capability, Error, ErrorKind, ObjectRef, ObjectStream, Reader, Result, async_trait,
};

#[derive(Clone)]
pub struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    pub error: Option<ErrorKind>,
    pub expected_options: Option<ByteReadOptions>,
}

impl MemoryAccessor {
    pub fn new(bytes: &[u8], chunk_size: usize) -> Self {
        Self {
            data: Bytes::copy_from_slice(bytes),
            chunk_size,
            error: None,
            expected_options: None,
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
        if let Some(expected) = self.expected_options {
            assert_eq!(request.options, expected);
        }
        Ok(Box::new(MemoryReader {
            metadata: ByteMetadata {
                source: Some(request.object),
                content_length: Some(self.data.len() as u64),
                version: Some("source-version".into()),
            },
            data: self.data.clone(),
            chunk_size: self.chunk_size,
            offset: 0,
            error: self.error,
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
    error: Option<ErrorKind>,
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
        if self.offset < self.data.len() {
            let end = (self.offset + self.chunk_size).min(self.data.len());
            let bytes = self.data.slice(self.offset..end);
            self.offset = end;
            return Ok(Some(bytes));
        }
        match self.error.take() {
            Some(kind) => Err(Error::new(kind, "upstream", "injected source failure")),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl ByteReader for MemoryReader {
    async fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Bytes> {
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

pub async fn collect(reader: &mut dyn ByteReader) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        assert!(!bytes.is_empty());
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}
