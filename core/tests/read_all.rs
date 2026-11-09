use open_arrow_access_core::{
    Accessor, ByteMetadata, ByteReader, Bytes, Capability, ErrorKind, Reader, Result, async_trait,
    read_all,
};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

struct LengthHint {
    inner: Box<dyn ByteReader>,
    metadata: ByteMetadata,
}

#[async_trait]
impl Reader for LengthHint {
    type Item = Bytes;
    type Metadata = ByteMetadata;
    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
    fn metadata(&self) -> &ByteMetadata {
        &self.metadata
    }
    async fn next(&mut self) -> Result<Option<Bytes>> {
        self.inner.next().await
    }
}
#[async_trait]
impl ByteReader for LengthHint {}

#[tokio::test]
async fn unknown_zero_underestimated_and_oversized_hints_do_not_change_bytes() {
    let bytes = fixture!("bytes", "alphabet.bin").await;
    for hint in [
        None,
        Some(0),
        Some(1),
        Some(bytes.len() as u64),
        Some(u64::MAX),
    ] {
        let inner = MemoryAccessor::new(&bytes, 3)
            .open("input".into())
            .await
            .unwrap();
        let mut metadata = inner.metadata().clone();
        metadata.content_length = hint;
        let output = read_all(Box::new(LengthHint { inner, metadata }), "test_all")
            .await
            .unwrap();
        assert_eq!(output, bytes);
        // A forged enormous length cannot trigger an enormous upfront allocation.
        assert!(output.capacity() <= 64 * 1024 * 1024);
    }
}

#[tokio::test]
async fn empty_input_stays_unallocated_and_source_contract_errors_are_preserved() {
    let bytes = fixture!("bytes", "empty.bin").await;
    let inner = MemoryAccessor::new(&bytes, 1)
        .open("input".into())
        .await
        .unwrap();
    let mut metadata = inner.metadata().clone();
    metadata.content_length = Some(u64::MAX);
    let output = read_all(Box::new(LengthHint { inner, metadata }), "test_all")
        .await
        .unwrap();
    assert!(output.is_empty());
    assert_eq!(output.capacity(), 0);
    let bytes = fixture!("bytes", "input.bin").await;
    let inner = MemoryAccessor::new(&bytes, 1)
        .with_empty_chunk(0)
        .open("input".into())
        .await
        .unwrap();
    let error = read_all(inner, "test_all").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "test_all");
    for offset in [0, 1] {
        let inner = MemoryAccessor::new(&bytes, 1)
            .with_failure(offset, ErrorKind::PermissionDenied)
            .open("input".into())
            .await
            .unwrap();
        let error = read_all(inner, "test_all").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(error.operation(), "memory_read");
    }
}
