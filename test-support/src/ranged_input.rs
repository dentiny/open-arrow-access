//! Contracts for consumers requiring exact ranges and a known input length.

use std::future::Future;

use open_arrow_access_core::{ErrorKind, Result};

use crate::{MemoryAccessor, reader::assert_memory_error};

/// Inspect valid fixture bytes through invalid or failing ranged sources.
/// The callback must perform an initial range read, not just construct an adapter.
/// Entry decoding, terminal reader behavior and format-specific failures stay local.
pub async fn assert_contract<T, F>(bytes: &[u8], inspect: impl Fn(MemoryAccessor) -> F)
where
    F: Future<Output = Result<T>>,
{
    assert!(!bytes.is_empty());
    let ranged = || MemoryAccessor::new(bytes, 1).with_ranges();
    for (source, expected, case) in [
        (
            MemoryAccessor::new(bytes, 1),
            ErrorKind::Unsupported,
            "sequential source",
        ),
        (
            ranged().without_content_length(),
            ErrorKind::Unsupported,
            "unknown length",
        ),
        (ranged().with_short_ranges(), ErrorKind::Io, "short range"),
    ] {
        let error = inspect(source).await.err().expect("accepted invalid input");
        assert_eq!(error.kind(), expected, "{case}: {error}");
    }
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        let error = inspect(ranged().with_range_failure(0, kind))
            .await
            .err()
            .expect("ignored injected range failure");
        assert_eq!(error.kind(), kind);
        assert_memory_error(&error, kind, "memory_range", "injected range failure");
    }
}
