//! Lifecycle and injected input-failure contracts independent of output items.

use std::{
    error::Error as _,
    fmt::Debug,
    sync::atomic::{AtomicUsize, Ordering},
};

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
            let error = loop {
                match reader.next().await {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("reader ignored an injected input failure"),
                    Err(error) => break error,
                }
            };
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

/// Exercise representative source-error categories and empty chunks at selected
/// byte offsets, with one-byte input chunks. Errors may occur during opening
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
    // Ordinary I/O and a non-I/O sentinel catch category erasure without
    // repeating the same adapter path for every enum variant.
    let kinds = [ErrorKind::Io, ErrorKind::PermissionDenied];
    for kind in kinds {
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
        assert!(offset <= bytes.len());
        for kind in kinds {
            let input = MemoryAccessor::new(bytes, 1).with_failure(offset, kind);
            let observed = input.observations.clone();
            let error = read_error(make_accessor(input), &request, &observed, &snapshot).await;
            assert_eq!(error.kind(), kind, "offset {offset}: {error}");
            assert_memory_read_error(&error, kind);
            check_source_error(&error, kind);
        }
        let input = MemoryAccessor::new(bytes, 1).with_empty_chunk(offset);
        let observed = input.observations.clone();
        let error = read_error(make_accessor(input), &request, &observed, &snapshot).await;
        assert_eq!(error.kind(), ErrorKind::Io, "offset {offset}: {error}");
    }
}

pub(crate) fn assert_memory_read_error(error: &Error, kind: ErrorKind) {
    assert_memory_error(error, kind, "memory_read", "injected source failure");
}

pub(crate) fn assert_memory_error(error: &Error, kind: ErrorKind, operation: &str, message: &str) {
    let original = memory_error(error, operation);
    assert_eq!(original.kind(), kind);
    assert_eq!(original.to_string(), format!("{operation}: {message}"));
    let cause = original
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
    assert_eq!(cause.to_string(), message);
}
