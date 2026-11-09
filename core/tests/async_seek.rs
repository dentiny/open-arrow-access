use std::{io::SeekFrom, num::NonZeroUsize};

use futures::io::{AsyncReadExt, AsyncSeekExt};
use open_arrow_access_core::{Accessor, ErrorKind, into_async_seek};
use open_arrow_access_test_support::{MemoryAccessor, fixture};

#[tokio::test]
async fn seeks_clones_and_direct_ranges_preserve_independent_cursors() {
    let source = MemoryAccessor::new(&fixture!("bytes", "alphabet.bin").await, 1).with_ranges();
    let observed = source.observations.clone();
    let bytes = source.open("data".into()).await.unwrap();
    let mut input = into_async_seek(bytes, NonZeroUsize::new(3).unwrap(), "seek_test").unwrap();
    assert!(observed.ranges.lock().unwrap().is_empty());
    for range in [11..11, 9..11, std::ops::Range { start: 3, end: 2 }] {
        let error = input.read_range(range).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "seek_test");
    }
    assert!(observed.ranges.lock().unwrap().is_empty());
    assert!(input.read_range(10..10).await.unwrap().is_empty());
    let mut head = [0; 2];
    input.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"ab");
    let mut fork = input.clone();
    fn owned_send<T: Send + 'static>(value: T) -> T {
        value
    }
    assert_eq!(
        owned_send(input.read_range(8..10)).await.unwrap().as_ref(),
        b"ij"
    );
    assert_eq!(input.seek(SeekFrom::Current(0)).await.unwrap(), 2);
    input.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"cd");
    fork.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"cd");
    input.seek(SeekFrom::End(-2)).await.unwrap();
    input.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"ij");
    assert_eq!(input.read(&mut head).await.unwrap(), 0);
    input.seek(SeekFrom::Start(1)).await.unwrap();
    input.read_exact(&mut head).await.unwrap();
    assert_eq!(&head, b"bc");
    assert!(input.seek(SeekFrom::Current(-3)).await.is_ok());
    assert!(input.seek(SeekFrom::Current(-1)).await.is_err());
    assert!(
        observed
            .ranges
            .lock()
            .unwrap()
            .iter()
            .all(|range| range.end - range.start <= 3)
    );
}

#[tokio::test]
async fn requirements_short_reads_and_original_errors_are_checked() {
    for source in [
        MemoryAccessor::new(&fixture!("bytes", "data.bin").await, 1),
        MemoryAccessor::new(&fixture!("bytes", "data.bin").await, 1)
            .with_ranges()
            .without_content_length(),
    ] {
        assert_eq!(
            into_async_seek(
                source.open("data".into()).await.unwrap(),
                NonZeroUsize::new(2).unwrap(),
                "seek_test"
            )
            .err()
            .unwrap()
            .kind(),
            ErrorKind::Unsupported
        );
    }
    for (source, expected) in [
        (
            MemoryAccessor::new(&fixture!("bytes", "data.bin").await, 1)
                .with_ranges()
                .with_short_ranges(),
            ErrorKind::Io,
        ),
        (
            MemoryAccessor::new(&fixture!("bytes", "data.bin").await, 1)
                .with_ranges()
                .with_range_failure(0, ErrorKind::PermissionDenied),
            ErrorKind::PermissionDenied,
        ),
    ] {
        let mut input = into_async_seek(
            source.open("data".into()).await.unwrap(),
            NonZeroUsize::new(2).unwrap(),
            "seek_test",
        )
        .unwrap();
        let error = input.read(&mut [0; 1]).await.unwrap_err();
        assert_eq!(ErrorKind::from_source(&error), Some(expected));
    }
}
