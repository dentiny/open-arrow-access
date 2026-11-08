//! Reusable error contract for sequential byte layers.

use crate::{MemoryAccessor, collect_bytes, reader};
use open_arrow_access_core::{Accessor, ByteReadRequest, ByteReader, Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Check exact output, nonempty bounded chunks, and EOF.
pub async fn assert_output(
    reader: &mut dyn ByteReader,
    expected: &[u8],
    max_chunk_size: usize,
) -> Result<()> {
    let mut output = Vec::new();
    while let Some(chunk) = reader.next().await? {
        assert!(!chunk.is_empty());
        assert!(chunk.len() <= max_chunk_size);
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, expected);
    assert!(reader.next().await?.is_none());
    Ok(())
}

/// Read the same representation through caller-selected input fragment sizes.
/// The factory configures the layer's output chunk size; this helper checks it.
pub async fn assert_fragmented_output<A>(
    make_layer: impl Fn(MemoryAccessor) -> A,
    bytes: &[u8],
    expected: &[u8],
    input_chunks: &[usize],
    max_output_chunk_size: usize,
) -> Result<()>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    assert!(!input_chunks.is_empty());
    for &chunk_size in input_chunks {
        let mut reader = make_layer(MemoryAccessor::new(bytes, chunk_size))
            .open("source".into())
            .await?;
        assert_output(reader.as_mut(), expected, max_output_chunk_size).await?;
    }
    Ok(())
}

/// Feed malformed input one byte at a time and require a terminal decode error.
pub async fn assert_decode_error<A>(
    make_layer: impl Fn(MemoryAccessor) -> A,
    bytes: &[u8],
    operation: &str,
) -> Result<()>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let input = MemoryAccessor::new(bytes, 1);
    let observed = input.observations.clone();
    let mut reader = make_layer(input).open("invalid".into()).await?;
    assert_terminal_error(
        reader.as_mut(),
        &observed.reads,
        ErrorKind::Decode,
        operation,
    )
    .await;
    Ok(())
}

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
    reader::assert_finished(reader, reads, |metadata| {
        (
            metadata.source.clone(),
            metadata.content_length,
            metadata.version.clone(),
        )
    })
    .await;
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
            reader::assert_memory_read_error(&error, kind);
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
