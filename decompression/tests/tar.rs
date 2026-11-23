use std::num::NonZeroUsize;

use open_arrow_access_core::{Accessor, ErrorKind};
use open_arrow_access_decompression::tar::{TarArchive, TarEntryReader};
use open_arrow_access_test_support::{
    MemoryAccessor, byte_layer, collect_bytes, expect_err_kind, fixture, ranged_input,
};

async fn source(name: &str) -> MemoryAccessor {
    MemoryAccessor::new(&fixture!("tar", name).await, 7)
        .with_ranges()
        .with_version("snapshot")
}

#[tokio::test]
async fn inspects_headers_once_and_streams_independent_selected_entries() {
    let source = source("mixed.tar").await;
    let archive = TarArchive::open(source.open("archive".into()).await.unwrap())
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
    assert_eq!(first.metadata().source, Some("archive".into()));
    assert_eq!(first.metadata().version.as_deref(), Some("snapshot"));
    assert_eq!(first.metadata().content_length, Some(entries[1].size));
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
    for (name, count) in [
        ("empty.tar", 0),
        ("gnu.tar", 1),
        ("gnu-longname.tar", 1),
        ("pax.tar", 1),
        ("link.tar", 1),
        ("one-zero-block.tar", 0),
        ("missing-end.tar", 4),
    ] {
        let source = source(name).await;
        let archive = TarArchive::open(source.open("archive".into()).await.unwrap())
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
        ("special.tar", ErrorKind::Unsupported),
    ] {
        let source = source(name).await;
        let error = TarArchive::open(source.open("archive".into()).await.unwrap())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), kind, "{name}");
        assert!(error.to_string().contains("archive"));
    }
}

#[tokio::test]
async fn enforces_exact_source_contract() {
    let bytes = fixture!("tar", "mixed.tar").await;
    ranged_input::assert_contract(&bytes, |source| async move {
        TarArchive::open(source.open("archive".into()).await?).await
    })
    .await;
}

#[tokio::test]
async fn preserves_entry_source_errors_and_terminates_reader() {
    let bytes = fixture!("tar", "mixed.tar").await;
    let source = MemoryAccessor::new(&bytes, 3)
        .with_ranges()
        .with_range_failure(6, ErrorKind::PermissionDenied);
    let archive = TarArchive::open(source.open("archive".into()).await.unwrap())
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
    let source = MemoryAccessor::new(&bytes, 64)
        .without_content_length()
        .with_version("snapshot");
    let mut archive = TarStream::new(source.open("archive".into()).await.unwrap());
    let TarEntryReader {
        entry: directory,
        reader: mut empty,
    } = archive.next_entry().await.unwrap().unwrap();
    assert!(!directory.is_file);
    assert!(empty.next().await.unwrap().is_none());
    let TarEntryReader {
        entry,
        reader: mut first,
    } = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.name, b"papers/paper.pdf");
    assert_eq!(first.metadata().source, Some("archive".into()));
    assert_eq!(first.metadata().version.as_deref(), Some("snapshot"));
    assert_eq!(first.metadata().content_length, Some(entry.size));
    assert_eq!(source.observations.reads.load(Ordering::SeqCst), 16);
    expect_err_kind(archive.next_entry().await, ErrorKind::InvalidInput);
    byte_layer::assert_output(first.as_mut(), b"%PDF-first-paper", 64)
        .await
        .unwrap();
    let TarEntryReader {
        entry: _,
        reader: mut second,
    } = archive.next_entry().await.unwrap().unwrap();
    // A consumed reader never resumes against the next entry's shared source.
    assert!(first.next().await.unwrap().is_none());
    byte_layer::assert_output(second.as_mut(), b"%PDF-second-paper", 64)
        .await
        .unwrap();
    let TarEntryReader {
        entry: _,
        reader: mut last,
    } = archive.next_entry().await.unwrap().unwrap();
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
    let mut archive = TarStream::new(source.open("archive".into()).await.unwrap());
    drop(archive.next_entry().await.unwrap().unwrap());
    let TarEntryReader {
        entry: _,
        reader: mut first,
    } = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), b"%P");
    drop(first);
    let TarEntryReader {
        entry,
        reader: mut second,
    } = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    drop(archive);
    byte_layer::assert_output(second.as_mut(), b"%PDF-second-paper", 3)
        .await
        .unwrap();
}

#[tokio::test]
async fn sequential_failures_are_terminal() {
    use open_arrow_access_decompression::tar::TarStream;

    for (name, kind) in [
        ("bad-checksum.tar", ErrorKind::Decode),
        ("bad-size.tar", ErrorKind::Decode),
    ] {
        let source = MemoryAccessor::new(&fixture!("tar", name).await, 7);
        let mut archive = TarStream::new(source.open("archive".into()).await.unwrap());
        expect_err_kind(archive.next_entry().await, kind);
        assert!(archive.next_entry().await.unwrap().is_none());
    }
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
    let mut archive = TarStream::new(input);
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    {
        let mut next = pin!(archive.next_entry());
        assert!(next.as_mut().poll(&mut context).is_pending());
    }
    allowed.store(11, Ordering::SeqCst);
    drop(archive.next_entry().await.unwrap().unwrap());
    let TarEntryReader {
        entry: _,
        reader: body,
    } = archive.next_entry().await.unwrap().unwrap();
    drop(body);
    // 1,100 bytes were fetched; skipping payload/padding needs more input.
    {
        let mut next = pin!(archive.next_entry());
        assert!(next.as_mut().poll(&mut context).is_pending());
    }
    allowed.store(usize::MAX, Ordering::SeqCst);
    let TarEntryReader {
        entry,
        reader: mut body,
    } = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    byte_layer::assert_output(body.as_mut(), b"%PDF-second-paper", 100)
        .await
        .unwrap();
    allowed.store(36, Ordering::SeqCst);
    drop(archive.next_entry().await.unwrap().unwrap());
    // Completion still waits for the underlying source/trailer to reach EOF.
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
        let mut archive = TarStream::new(source.open("archive".into()).await.unwrap());
        drop(archive.next_entry().await.unwrap().unwrap());
        let TarEntryReader {
            entry: _,
            reader: mut body,
        } = archive.next_entry().await.unwrap().unwrap();
        expect_err_kind(collect_bytes(body.as_mut()).await, kind);
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
    let mut archive = TarStream::new(input);
    drop(archive.next_entry().await.unwrap().unwrap());
    let TarEntryReader {
        entry: _,
        reader: mut body,
    } = archive.next_entry().await.unwrap().unwrap();
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
    let TarEntryReader {
        entry,
        reader: mut body,
    } = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.index, 2);
    byte_layer::assert_output(body.as_mut(), b"%PDF-second-paper", 512)
        .await
        .unwrap();
}

#[tokio::test]
async fn sequential_completion_and_malformed_headers_release_source_with_archive_retained() {
    use open_arrow_access_core::{ByteMetadata, Bytes, StreamReader};
    use open_arrow_access_decompression::tar::TarStream;
    use open_arrow_access_test_support::DropProbe;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
    };
    for (name, malformed) in [("mixed.tar", false), ("bad-checksum.tar", true)] {
        let dropped = Arc::new(AtomicBool::new(false));
        let signal = DropProbe::new(dropped.clone());
        let mut bytes = Some(Bytes::from(fixture!("tar", name).await));
        let source = futures::stream::poll_fn(move |_| {
            let _keep = &signal;
            bytes
                .take()
                .map_or(Poll::Ready(None), |bytes| Poll::Ready(Some(Ok(bytes))))
        });
        let input = Box::new(StreamReader::new(source, ByteMetadata::default()));
        let mut archive = TarStream::new(input);
        assert!(!dropped.load(Ordering::SeqCst));
        if malformed {
            expect_err_kind(archive.next_entry().await, ErrorKind::Decode);
        } else {
            while let Some(TarEntryReader {
                entry: _,
                reader: mut body,
            }) = archive.next_entry().await.unwrap()
            {
                collect_bytes(body.as_mut()).await.unwrap();
            }
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert!(archive.next_entry().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn gnu_long_names_match_in_indexed_and_sequential_reads() {
    use open_arrow_access_decompression::tar::TarStream;
    let input = source("gnu-longname.tar").await;
    let indexed = TarArchive::open(input.open("archive".into()).await.unwrap())
        .await
        .unwrap();
    let mut streamed = TarStream::new(input.open("archive".into()).await.unwrap());
    let mut member = streamed.next_entry().await.unwrap().unwrap();
    assert_eq!(&member.entry, indexed.entries().next().unwrap());
    assert_eq!(member.entry.index, 0);
    assert_eq!(member.entry.name, vec![b'x'; 120]);
    assert_eq!(collect_bytes(member.reader.as_mut()).await.unwrap(), b"x");
    assert!(streamed.next_entry().await.unwrap().is_none());
    assert_eq!(
        collect_bytes(indexed.open_entry(0, NonZeroUsize::MIN).unwrap().as_mut())
            .await
            .unwrap(),
        b"x"
    );
}

#[tokio::test]
async fn cancelled_gnu_long_name_reads_resume() {
    use futures::FutureExt;
    use open_arrow_access_core::Bytes;
    use open_arrow_access_decompression::tar::TarStream;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let allowed = Arc::new(AtomicUsize::new(6));
    let input = gated_source(
        Bytes::from(fixture!("tar", "gnu-longname.tar").await),
        100,
        allowed.clone(),
    );
    let mut archive = TarStream::new(input);
    // Cancel during the name payload, its padding, and the following header.
    for chunks in [6, 7, 11] {
        allowed.store(chunks, Ordering::SeqCst);
        assert!(archive.next_entry().now_or_never().is_none());
    }
    allowed.store(usize::MAX, Ordering::SeqCst);
    let mut member = archive.next_entry().await.unwrap().unwrap();
    assert_eq!(member.entry.index, 0);
    assert_eq!(member.entry.name, vec![b'x'; 120]);
    assert_eq!(collect_bytes(member.reader.as_mut()).await.unwrap(), b"x");
    assert!(archive.next_entry().await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_gnu_names_fail_in_both_readers() {
    use open_arrow_access_decompression::tar::TarStream;
    let original = fixture!("tar", "gnu-longname.tar").await;
    let mut orphan = original[..1024].to_vec();
    orphan.extend_from_slice(&[0; 1024]);
    let mut consecutive = original[..1024].to_vec();
    consecutive.extend_from_slice(&original);
    for bytes in [orphan, consecutive] {
        let input = MemoryAccessor::new(&bytes, 7).with_ranges();
        expect_err_kind(
            TarArchive::open(input.open("archive".into()).await.unwrap()).await,
            ErrorKind::Decode,
        );
        let mut stream = TarStream::new(input.open("archive".into()).await.unwrap());
        expect_err_kind(stream.next_entry().await, ErrorKind::Decode);
        assert!(stream.next_entry().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn link_metadata_never_dereferences_targets_and_preserves_following_files() {
    use open_arrow_access_decompression::tar::TarStream;
    let input = source("links-and-files.tar").await;
    let indexed = TarArchive::open(input.open("archive".into()).await.unwrap())
        .await
        .unwrap();
    let headers: Vec<_> = indexed.entries().cloned().collect();
    assert_eq!(headers.len(), 4);
    let mut stream = TarStream::new(input.open("archive".into()).await.unwrap());
    for (index, header) in headers.iter().enumerate() {
        let mut member = stream.next_entry().await.unwrap().unwrap();
        assert_eq!(&member.entry, header);
        assert_eq!(header.index, index);
        if header.is_file {
            let expected = if index == 0 {
                b"first".as_slice()
            } else {
                b"last".as_slice()
            };
            assert_eq!(
                collect_bytes(member.reader.as_mut()).await.unwrap(),
                expected
            );
            assert_eq!(
                collect_bytes(
                    indexed
                        .open_entry(index, NonZeroUsize::MIN)
                        .unwrap()
                        .as_mut()
                )
                .await
                .unwrap(),
                expected
            );
        } else {
            expect_err_kind(indexed.entry_range(index), ErrorKind::Unsupported);
            assert!(indexed.open_entry(index, NonZeroUsize::MIN).is_err());
            assert!(
                collect_bytes(member.reader.as_mut())
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
    assert!(stream.next_entry().await.unwrap().is_none());
    assert_eq!(input.observations.requests.lock().unwrap().len(), 2);
}
