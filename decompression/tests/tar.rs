use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::tar::{TarArchive, TarLimits};
use open_arrow_access_test_support::{MemoryAccessor, byte_layer, collect_bytes, fixture};

async fn source(name: &str) -> MemoryAccessor {
    MemoryAccessor::new(&fixture!("tar", name).await, 7).with_ranges()
}

#[tokio::test]
async fn inspects_headers_once_and_streams_independent_selected_entries() {
    let source = source("mixed.tar").await;
    let archive = TarArchive::open(
        source.open("archive".into()).await.unwrap(),
        TarLimits::default(),
    )
    .await
    .unwrap();
    let entries: Vec<_> = archive.entries().collect();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[1].name, b"papers/paper.pdf");
    assert_eq!(entries[2].name, entries[1].name);
    assert!(!entries[0].is_file);
    assert!(
        source
            .observations
            .ranges
            .lock()
            .unwrap()
            .iter()
            .all(|range| range.end - range.start == 512)
    );
    assert_eq!(source.observations.requests.lock().unwrap().len(), 1);
    assert_eq!(
        archive
            .open_entry(0, NonZeroUsize::new(3).unwrap())
            .err()
            .unwrap()
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        archive
            .open_entry(9, NonZeroUsize::new(3).unwrap())
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    let mut first = archive
        .open_entry(1, NonZeroUsize::new(3).unwrap())
        .unwrap();
    let mut second = archive
        .open_entry(2, NonZeroUsize::new(5).unwrap())
        .unwrap();
    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), b"%PD");
    assert_eq!(second.next().await.unwrap().unwrap().as_ref(), b"%PDF-");
    let mut empty = archive.open_entry(3, NonZeroUsize::MIN).unwrap();
    assert_eq!(empty.metadata().content_length, Some(0));
    assert!(empty.next().await.unwrap().is_none());
    drop(archive);
    byte_layer::assert_output(first.as_mut(), b"F-first-paper", 3)
        .await
        .unwrap();
    byte_layer::assert_output(second.as_mut(), b"second-paper", 5)
        .await
        .unwrap();
}

#[tokio::test]
async fn accepts_empty_and_gnu_archives() {
    for (name, count) in [("empty.tar", 0), ("gnu.tar", 1)] {
        let source = source(name).await;
        let archive = TarArchive::open(
            source.open("archive".into()).await.unwrap(),
            TarLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(archive.entries().len(), count);
    }
}

#[tokio::test]
async fn rejects_malformed_and_unsupported_archives() {
    for (name, kind) in [
        ("bad-checksum.tar", ErrorKind::Decode),
        ("bad-size.tar", ErrorKind::Decode),
        ("truncated.tar", ErrorKind::Decode),
        ("missing-end.tar", ErrorKind::Decode),
        ("one-zero-block.tar", ErrorKind::Decode),
        ("link.tar", ErrorKind::Unsupported),
        ("pax.tar", ErrorKind::Unsupported),
        ("gnu-longname.tar", ErrorKind::Unsupported),
    ] {
        let source = source(name).await;
        assert_eq!(
            TarArchive::open(
                source.open("archive".into()).await.unwrap(),
                TarLimits::default()
            )
            .await
            .err()
            .unwrap()
            .kind(),
            kind,
            "{name}"
        );
    }
}

#[tokio::test]
async fn enforces_limits_and_exact_source_contract() {
    let source = source("mixed.tar").await;
    for limits in [
        TarLimits {
            max_entries: 1,
            ..Default::default()
        },
        TarLimits {
            max_entry_bytes: 2,
            ..Default::default()
        },
    ] {
        assert_eq!(
            TarArchive::open(source.open("archive".into()).await.unwrap(), limits)
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
    }
    let bytes = fixture!("tar", "mixed.tar").await;
    for source in [
        MemoryAccessor::new(&bytes, 3),
        MemoryAccessor::new(&bytes, 3)
            .with_ranges()
            .without_content_length(),
    ] {
        assert_eq!(
            TarArchive::open(
                source.open(ByteReadRequest::new("archive")).await.unwrap(),
                TarLimits::default()
            )
            .await
            .err()
            .unwrap()
            .kind(),
            ErrorKind::Unsupported
        );
    }
    let source = MemoryAccessor::new(&bytes, 3)
        .with_ranges()
        .with_short_ranges();
    assert_eq!(
        TarArchive::open(
            source.open("archive".into()).await.unwrap(),
            TarLimits::default()
        )
        .await
        .err()
        .unwrap()
        .kind(),
        ErrorKind::Io
    );
}

#[tokio::test]
async fn preserves_entry_source_errors_and_terminates_reader() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 3)
        .with_ranges()
        .with_range_failure(6, ErrorKind::PermissionDenied);
    let archive = TarArchive::open(
        source.open("archive".into()).await.unwrap(),
        TarLimits::default(),
    )
    .await
    .unwrap();
    let mut reader = archive
        .open_entry(1, NonZeroUsize::new(4).unwrap())
        .unwrap();
    byte_layer::assert_terminal_error(
        reader.as_mut(),
        &source.observations.range_reads,
        ErrorKind::PermissionDenied,
        "memory_range",
    )
    .await;
}

#[tokio::test]
async fn streams_first_body_without_scanning_later_headers_or_using_ranges() {
    use open_arrow_access_decompression::tar::TarStream;
    use std::sync::atomic::Ordering;

    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 64).without_content_length();
    let mut archive = TarStream::new(
        source.open("archive".into()).await.unwrap(),
        TarLimits::default(),
    );
    let (directory, mut empty) = archive.next_entry().await.unwrap().unwrap();
    assert!(!directory.is_file);
    assert!(empty.next().await.unwrap().is_none());
    let (entry, mut first) = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.name, b"papers/paper.pdf");
    assert_eq!(source.observations.reads.load(Ordering::SeqCst), 16);
    assert_eq!(
        archive.next_entry().await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        collect_bytes(first.as_mut()).await.unwrap(),
        b"%PDF-first-paper"
    );
    let (_, mut second) = archive.next_entry().await.unwrap().unwrap();
    // A consumed reader never resumes against the next entry's shared source.
    assert!(first.next().await.unwrap().is_none());
    assert_eq!(
        collect_bytes(second.as_mut()).await.unwrap(),
        b"%PDF-second-paper"
    );
    let (_, mut last) = archive.next_entry().await.unwrap().unwrap();
    assert!(last.next().await.unwrap().is_none());
    assert!(archive.next_entry().await.unwrap().is_none());
    assert!(archive.next_entry().await.unwrap().is_none());
    assert_eq!(source.observations.range_reads.load(Ordering::SeqCst), 0);
    assert_eq!(source.observations.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn drains_dropped_bodies_and_readers_outlive_sequential_archive() {
    use open_arrow_access_decompression::tar::TarStream;

    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 3);
    let mut archive = TarStream::new(
        source.open("archive".into()).await.unwrap(),
        TarLimits::default(),
    );
    drop(archive.next_entry().await.unwrap().unwrap());
    let (_, mut first) = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), b"%P");
    drop(first);
    let (entry, mut second) = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    drop(archive);
    assert_eq!(
        collect_bytes(second.as_mut()).await.unwrap(),
        b"%PDF-second-paper"
    );
}

#[tokio::test]
async fn sequential_failures_are_terminal_and_share_header_limits() {
    use open_arrow_access_decompression::tar::TarStream;

    for (name, kind) in [
        ("bad-checksum.tar", ErrorKind::Decode),
        ("bad-size.tar", ErrorKind::Decode),
        ("one-zero-block.tar", ErrorKind::Decode),
        ("pax.tar", ErrorKind::Unsupported),
    ] {
        let source = MemoryAccessor::new(&fixture!("tar", name).await, 7);
        let mut archive = TarStream::new(
            source.open("archive".into()).await.unwrap(),
            TarLimits::default(),
        );
        assert_eq!(archive.next_entry().await.err().unwrap().kind(), kind);
        assert!(archive.next_entry().await.unwrap().is_none());
    }
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 64);
    let mut archive = TarStream::new(
        source.open("archive".into()).await.unwrap(),
        TarLimits {
            max_entries: 1,
            ..Default::default()
        },
    );
    drop(archive.next_entry().await.unwrap().unwrap());
    assert_eq!(
        archive.next_entry().await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    assert!(archive.next_entry().await.unwrap().is_none());
}

#[tokio::test]
async fn cancelled_sequential_header_and_skip_resume_from_persistent_progress() {
    use futures::Future;
    use open_arrow_access_core::Bytes;
    use open_arrow_access_decompression::tar::TarStream;
    use std::{
        pin::pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Context,
    };

    let bytes = Bytes::from(fixture!("tar", "mixed.tar").await);
    let allowed = Arc::new(AtomicUsize::new(1));
    let input = gated_source(bytes, 100, allowed.clone());
    let mut archive = TarStream::new(input, TarLimits::default());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    {
        let mut next = pin!(archive.next_entry());
        assert!(next.as_mut().poll(&mut context).is_pending());
    }
    allowed.store(11, Ordering::SeqCst);
    drop(archive.next_entry().await.unwrap().unwrap());
    let (_, body) = archive.next_entry().await.unwrap().unwrap();
    drop(body);
    // 1,100 bytes were fetched; skipping payload/padding needs more input.
    {
        let mut next = pin!(archive.next_entry());
        assert!(next.as_mut().poll(&mut context).is_pending());
    }
    allowed.store(usize::MAX, Ordering::SeqCst);
    let (entry, mut body) = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    assert_eq!(
        collect_bytes(body.as_mut()).await.unwrap(),
        b"%PDF-second-paper"
    );
    allowed.store(36, Ordering::SeqCst);
    drop(archive.next_entry().await.unwrap().unwrap());
    // The first zero marker is consumed, with only a partial second marker.
    {
        let mut next = pin!(archive.next_entry());
        assert!(next.as_mut().poll(&mut context).is_pending());
    }
    allowed.store(usize::MAX, Ordering::SeqCst);
    assert!(archive.next_entry().await.unwrap().is_none());
}

#[tokio::test]
async fn sequential_body_source_errors_and_truncation_terminate_archive() {
    use open_arrow_access_decompression::tar::TarStream;

    let bytes = fixture!("tar", "mixed.tar").await;
    for (source, kind) in [
        (
            MemoryAccessor::new(&bytes, 64).with_failure(1024, ErrorKind::PermissionDenied),
            ErrorKind::PermissionDenied,
        ),
        (
            MemoryAccessor::new(&bytes, 64).with_empty_chunk(1024),
            ErrorKind::Io,
        ),
        (MemoryAccessor::new(&bytes[..1030], 64), ErrorKind::Decode),
    ] {
        let mut archive = TarStream::new(
            source.open("archive".into()).await.unwrap(),
            TarLimits::default(),
        );
        drop(archive.next_entry().await.unwrap().unwrap());
        let (_, mut body) = archive.next_entry().await.unwrap().unwrap();
        assert_eq!(
            collect_bytes(body.as_mut()).await.err().unwrap().kind(),
            kind
        );
        assert!(body.next().await.unwrap().is_none());
        assert!(archive.next_entry().await.unwrap().is_none());
    }
}

fn gated_source(
    bytes: open_arrow_access_core::Bytes,
    chunk_size: usize,
    allowed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> Box<dyn open_arrow_access_core::ByteReader> {
    use open_arrow_access_core::{ByteMetadata, StreamReader};
    use std::{sync::atomic::Ordering, task::Poll};
    let mut position = 0;
    let source = futures::stream::poll_fn(move |_| {
        if position / chunk_size >= allowed.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        if position == bytes.len() {
            return Poll::Ready(None);
        }
        let end = (position + chunk_size).min(bytes.len());
        let chunk = bytes.slice(position..end);
        position = end;
        Poll::Ready(Some(Ok(chunk)))
    });
    Box::new(StreamReader::new(source, ByteMetadata::default()))
}

#[tokio::test]
async fn cancelled_live_body_read_rejects_advancement_without_waiting_for_its_lock() {
    use futures::{Future, FutureExt};
    use open_arrow_access_core::Bytes;
    use open_arrow_access_decompression::tar::TarStream;
    use std::{
        pin::pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Context,
    };

    let allowed = Arc::new(AtomicUsize::new(2));
    let input = gated_source(
        Bytes::from(fixture!("tar", "mixed.tar").await),
        512,
        allowed.clone(),
    );
    let mut archive = TarStream::new(input, TarLimits::default());
    drop(archive.next_entry().await.unwrap().unwrap());
    let (_, mut body) = archive.next_entry().await.unwrap().unwrap();
    {
        let mut next = pin!(body.next());
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
                .is_pending()
        );
    }
    assert_eq!(
        archive
            .next_entry()
            .now_or_never()
            .expect("must not wait for an active reader")
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
    drop(body);
    allowed.store(usize::MAX, Ordering::SeqCst);
    let (entry, mut body) = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    assert_eq!(
        collect_bytes(body.as_mut()).await.unwrap(),
        b"%PDF-second-paper"
    );
}

#[tokio::test]
async fn sequential_completion_and_malformed_headers_release_source_with_archive_retained() {
    use open_arrow_access_core::{ByteMetadata, Bytes, StreamReader};
    use open_arrow_access_decompression::tar::TarStream;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
    };
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    for (name, malformed) in [("mixed.tar", false), ("bad-checksum.tar", true)] {
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = Dropped(dropped.clone());
        let mut bytes = Some(Bytes::from(fixture!("tar", name).await));
        let source = futures::stream::poll_fn(move |_| {
            let _keep = &signal;
            bytes
                .take()
                .map_or(Poll::Pending, |bytes| Poll::Ready(Some(Ok(bytes))))
        });
        let input = Box::new(StreamReader::new(source, ByteMetadata::default()));
        let mut archive = TarStream::new(input, TarLimits::default());
        assert!(!dropped.load(Ordering::SeqCst));
        if malformed {
            assert_eq!(
                archive.next_entry().await.err().unwrap().kind(),
                ErrorKind::Decode
            );
        } else {
            while let Some((_, mut body)) = archive.next_entry().await.unwrap() {
                collect_bytes(body.as_mut()).await.unwrap();
            }
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert!(archive.next_entry().await.unwrap().is_none());
    }
}
