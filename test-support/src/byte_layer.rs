//! Reusable error contract for sequential byte layers.

use crate::{MemoryAccessor, collect_bytes, memory_read_error};
use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Consume a failing reader and verify that subsequent calls do not poll upstream.
pub async fn assert_terminal_error(
    reader: &mut dyn ByteReader,
    reads: &AtomicUsize,
    kind: ErrorKind,
    operation: &str,
) -> Error {
    use std::error::Error as _;
    let error = collect_bytes(reader).await.unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), operation);
    assert!(error.source().is_some());
    let calls = reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reads.load(Ordering::SeqCst), calls);
    error
}

/// Verify error categories, source chains, terminal reads, and removed range support
/// for a sequential byte layer built over the shared memory backend.
/// `bytes` must encode `expected` in the representation accepted by the layer.
pub async fn assert_error_contract<A>(
    make_layer: impl Fn(MemoryAccessor) -> A,
    bytes: &[u8],
    expected: &[u8],
    operation: &str,
) -> Result<()>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    use std::error::Error as _;
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        // Fail before input, after its first byte, and while checking EOF.
        for offset in [0, 1, bytes.len()] {
            let input = MemoryAccessor::new(bytes, 1).with_failure(offset, kind);
            let observed = input.observations.clone();
            let mut reader = make_layer(input).open("source".into()).await?;
            assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
            let error =
                assert_terminal_error(reader.as_mut(), &observed.reads, kind, operation).await;
            let original = memory_read_error(&error);
            assert_eq!(original.kind(), kind);
            assert_eq!(original.to_string(), "memory_read: injected source failure");
            let cause = original
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap();
            assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
            assert_eq!(cause.to_string(), "injected source failure");
        }
    }
    for offset in [0, 1, bytes.len()] {
        let input = MemoryAccessor::new(bytes, 1).with_empty_chunk(offset);
        let observed = input.observations.clone();
        let mut reader = make_layer(input).open("empty-chunk".into()).await?;
        let error =
            assert_terminal_error(reader.as_mut(), &observed.reads, ErrorKind::Io, operation).await;
        assert!(error.source().unwrap().to_string().contains("empty chunk"));
    }
    let input = MemoryAccessor::new(bytes, 1);
    let mut reader = make_layer(input).open("sequential".into()).await?;
    let error = reader.read_range(0..1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), "read_range");
    let error = reader.read_ranges(vec![0..1, 1..2]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(reader.read_ranges(vec![]).await?.is_empty());
    assert_eq!(collect_bytes(reader.as_mut()).await?, expected);
    Ok(())
}
