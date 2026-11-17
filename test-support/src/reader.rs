//! Lifecycle and injected input-failure contracts independent of output items.

use std::{
    error::Error as _,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

use more_asserts::assert_le;
use open_arrow_access_core::{Accessor, ByteReadRequest, Error, ErrorKind, Reader};

use crate::{MemoryAccessor, Observations, memory_error};

/// After an observed EOF or error, require stable metadata and no upstream polls.
/// The snapshot selects comparable metadata without imposing traits on the
/// reader's metadata or item type. Include every field whose stability matters.
pub async fn assert_finished<R, M>(
    reader: &mut R,
    reads: &AtomicUsize,
    snapshot: impl Fn(&R::Metadata) -> M,
) where
    R: Reader + ?Sized,
    M: Debug + PartialEq,
{
    let calls = reads.load(Ordering::SeqCst);
    let metadata = snapshot(reader.metadata());
    for _ in 0..2 {
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(reads.load(Ordering::SeqCst), calls);
        assert_eq!(snapshot(reader.metadata()), metadata);
    }
}

/// Require the next call to fail, then verify terminal behavior. This does not
/// drain successful items; callers can inspect parser-specific errors themselves.
pub async fn assert_terminal_error<R, M>(
    reader: &mut R,
    reads: &AtomicUsize,
    kind: ErrorKind,
    snapshot: impl Fn(&R::Metadata) -> M,
) -> Error
where
    R: Reader + ?Sized,
    M: Debug + PartialEq,
{
    let error = match reader.next().await {
        Err(error) => error,
        Ok(_) => panic!("reader did not return the expected error"),
    };
    assert_eq!(error.kind(), kind);
    assert_finished(reader, reads, snapshot).await;
    error
}

/// Drain successful items and return the first reader error.
/// Panics if the reader reaches EOF before reporting an error. Terminal-state
/// and metadata assertions remain the caller's responsibility.
pub async fn next_error<R: Reader + ?Sized>(reader: &mut R) -> Error {
    loop {
        match reader.next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("reader reached EOF before the expected error"),
            Err(error) => return error,
        }
    }
}

async fn read_error<A, M>(
    accessor: A,
    request: &A::Request,
    observed: &Observations,
    snapshot: &impl Fn(&<A::Reader as Reader>::Metadata) -> M,
) -> Error
where
    A: Accessor,
    A::Request: Clone,
    for<'a> ByteReadRequest: From<&'a A::Request>,
    M: Debug + PartialEq,
{
    let error = match accessor.open(request.clone()).await {
        Err(error) => error,
        Ok(mut reader) => {
            let error = next_error(reader.as_mut()).await;
            assert_finished(reader.as_mut(), &observed.reads, snapshot).await;
            error
        }
    };
    let expected = ByteReadRequest::from(request);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].object, expected.object);
    assert_eq!(requests[0].options, expected.options);
    error
}

/// Exercise source errors at selected byte offsets and one empty chunk, using
/// one-byte input chunks. Errors may occur during opening
/// (such as schema inference) or while reading. Open failures must not poll input.
/// The request's byte conversion defines the expected forwarded object/options.
///
/// Supply valid input and reachable offsets. `snapshot` captures stable metadata;
/// `check_source_error` adds parser-specific assertions for injected read errors.
/// Range-based consumers retain their own range-failure scenarios.
pub async fn assert_error_contract<A, M>(
    make_accessor: impl Fn(MemoryAccessor) -> A,
    bytes: &[u8],
    request: A::Request,
    offsets: &[usize],
    snapshot: impl Fn(&<A::Reader as Reader>::Metadata) -> M,
    check_source_error: impl Fn(&Error, ErrorKind),
) where
    A: Accessor,
    A::Request: Clone,
    for<'a> ByteReadRequest: From<&'a A::Request>,
    M: Debug + PartialEq,
{
    assert!(!offsets.is_empty());
    // A non-I/O sentinel detects adapters that erase source categories to Io.
    // Opening and reading are distinct paths; enum variants are not.
    let kind = ErrorKind::PermissionDenied;
    {
        let input = MemoryAccessor::new(bytes, 1).with_open_failure(kind);
        let observed = input.observations.clone();
        let error = read_error(make_accessor(input), &request, &observed, &snapshot).await;
        assert_eq!(error.kind(), kind);
        assert_eq!(error.operation(), "memory_open");
        let cause = error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    }
    for &offset in offsets {
        assert_le!(offset, bytes.len());
        let input = MemoryAccessor::new(bytes, 1).with_failure(offset, kind);
        let observed = input.observations.clone();
        let error = read_error(make_accessor(input), &request, &observed, &snapshot).await;
        assert_eq!(error.kind(), kind, "offset {offset}: {error}");
        assert_memory_read_error(&error, kind);
        check_source_error(&error, kind);
    }
    let input = MemoryAccessor::new(bytes, 1).with_empty_chunk(0);
    let observed = input.observations.clone();
    let error = read_error(make_accessor(input), &request, &observed, &snapshot).await;
    assert_eq!(error.kind(), ErrorKind::Io);
}

pub(crate) fn assert_memory_read_error(error: &Error, kind: ErrorKind) {
    assert_memory_error(error, kind, "memory_read", "injected source failure");
}

pub(crate) fn assert_memory_error(error: &Error, kind: ErrorKind, operation: &str, message: &str) {
    let original = memory_error(error, operation);
    assert_eq!(original.kind(), kind);
    assert_eq!(original.operation(), operation);
    assert!(original.to_string().ends_with(&format!(": {message}")));
    let cause = original
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
    assert_eq!(cause.to_string(), message);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn next_error_drains_successes_and_preserves_the_failure() {
        let input = MemoryAccessor::new(b"ab", 1).with_failure(1, ErrorKind::PermissionDenied);
        let observed = input.observations.clone();
        let mut reader = input.open("input".into()).await.unwrap();
        let error = next_error(reader.as_mut()).await;
        assert_memory_read_error(&error, ErrorKind::PermissionDenied);
        assert_eq!(observed.reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[should_panic(expected = "reader reached EOF before the expected error")]
    async fn next_error_rejects_successful_eof() {
        let input = MemoryAccessor::new(b"", 1);
        let mut reader = input.open("input".into()).await.unwrap();
        next_error(reader.as_mut()).await;
    }
}
