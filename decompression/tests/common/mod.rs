pub fn fixture(format: &str, name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("testdata")
        .join(format)
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use open_arrow_access_decompression::{Codec, DecompressionLayer};

use open_arrow_access_core::{
    Accessor, AccessorExt, ByteAccessor, ByteMetadata, ByteReadOptions, ByteReadRequest,
    ByteReader, Bytes, Capability, Error, ErrorKind, ObjectRef, ObjectStream, Reader, Result,
    async_trait,
};

#[derive(Clone)]
pub struct MemoryAccessor {
    data: Bytes,
    chunk_size: usize,
    pub error: Option<ErrorKind>,
    pub fail_after: Option<usize>,
    pub empty_chunk_after: Option<usize>,
    pub reads: Arc<AtomicUsize>,
    pub expected_options: Option<ByteReadOptions>,
}

impl MemoryAccessor {
    pub fn new(bytes: &[u8], chunk_size: usize) -> Self {
        Self {
            data: Bytes::copy_from_slice(bytes),
            chunk_size,
            error: None,
            fail_after: None,
            empty_chunk_after: None,
            reads: Arc::default(),
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
            fail_after: self.fail_after,
            empty_chunk_after: self.empty_chunk_after,
            reads: self.reads.clone(),
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
    fail_after: Option<usize>,
    empty_chunk_after: Option<usize>,
    reads: Arc<AtomicUsize>,
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
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.empty_chunk_after == Some(self.offset) {
            self.empty_chunk_after = None;
            return Ok(Some(Bytes::new()));
        }
        if self.fail_after.is_some_and(|offset| self.offset >= offset) {
            return Err(source_error(self.error.take().unwrap()));
        }
        if self.offset < self.data.len() {
            let end = (self.offset + self.chunk_size).min(self.data.len());
            let bytes = self.data.slice(self.offset..end);
            self.offset = end;
            return Ok(Some(bytes));
        }
        match self.error.take() {
            Some(kind) => Err(source_error(kind)),
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

fn source_error(kind: ErrorKind) -> Error {
    Error::new(kind, "upstream", "injected source failure").with_source(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "fixture connection reset",
    ))
}

fn upstream_error(error: &Error) -> &Error {
    use std::error::Error as _;
    let mut source = error.source().expect("missing decoder source");
    loop {
        if let Some(shared) = source.downcast_ref::<Error>()
            && shared.operation() == "upstream"
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

pub async fn assert_terminal_error(
    reader: &mut dyn ByteReader,
    reads: &AtomicUsize,
    kind: ErrorKind,
) -> Error {
    use std::error::Error as _;
    let error = collect(reader).await.unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "decompress");
    assert!(error.source().is_some());
    let calls = reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads.load(Ordering::SeqCst), calls);
    error
}

pub async fn error_contract<C: Codec>(codec: C, format: &str, payload: &str) -> Result<()> {
    use std::error::Error as _;
    let bytes = fixture(format, payload);
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        // Fail before a header, within it, and while checking EOF after a valid stream.
        for offset in [0, 1, bytes.len()] {
            let mut input = MemoryAccessor::new(&bytes, 1);
            input.error = Some(kind);
            input.fail_after = Some(offset);
            let reads = input.reads.clone();
            let mut reader = input
                .layer(DecompressionLayer::new(codec.clone()))
                .open("source".into())
                .await?;
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            let error = assert_terminal_error(reader.as_mut(), &reads, kind).await;
            let original = upstream_error(&error);
            assert_eq!(original.kind(), kind);
            assert_eq!(original.to_string(), "upstream: injected source failure");
            let cause = original
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
            assert_eq!(cause.to_string(), "fixture connection reset");
        }
    }
    for offset in [0, 1, bytes.len()] {
        let mut input = MemoryAccessor::new(&bytes, 1);
        input.empty_chunk_after = Some(offset);
        let reads = input.reads.clone();
        let mut reader = input
            .layer(DecompressionLayer::new(codec.clone()))
            .open("empty-chunk".into())
            .await?;
        let error = assert_terminal_error(reader.as_mut(), &reads, ErrorKind::Io).await;
        assert!(error.source().unwrap().to_string().contains("empty chunk"));
    }
    let input = MemoryAccessor::new(&bytes, 1);
    let mut reader = input
        .layer(DecompressionLayer::new(codec))
        .open("sequential".into())
        .await?;
    let error = reader.read_range(0..1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), "read_range");
    let error = reader.read_ranges(vec![0..1, 1..2]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(reader.read_ranges(vec![]).await?.is_empty());
    assert_eq!(collect(reader.as_mut()).await?, b"payload");
    Ok(())
}
