mod common;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::Poll;

use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{Capability, Error, ErrorKind, Reader, StreamReader, into_stream};
use open_arrow_access_test_support::DropProbe;

use common::FailingReader;

#[tokio::test]
async fn stream_stops_after_error_without_polling_reader_again() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut stream = into_stream(Box::new(FailingReader::new(calls.clone())));
    assert_eq!(stream.try_next().await.err().unwrap().kind(), ErrorKind::Io);
    assert!(stream.next().await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_reader_is_lazy_preserves_order_and_fuses_non_unpin_streams() {
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = DropProbe::new(dropped.clone());
    let observed = calls.clone();
    // Deliberately resume producing items after EOF to exercise the adapter's fuse.
    let stream = stream::poll_fn(move |_| {
        let _keep_alive = &guard;
        Poll::Ready(match observed.fetch_add(1, Ordering::SeqCst) {
            0 => Some(Ok(10)),
            1 => Some(Ok(20)),
            2 => None,
            _ => Some(Ok(30)),
        })
    })
    // The async block makes this stream !Unpin.
    .then(|item| async move { item });
    let mut reader = StreamReader::new(stream, String::from("source"));
    assert_eq!(reader.metadata(), "source");
    assert_eq!(
        reader.capability(),
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(reader.next().await.unwrap(), Some(10));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader.next().await.unwrap(), Some(20));
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(reader.metadata(), "source");
}

#[tokio::test]
async fn stream_reader_preserves_errors_and_releases_the_stream_before_returning() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = DropProbe::new(dropped.clone());
    let stream = stream::poll_fn(move |_| {
        let _keep_alive = &guard;
        Poll::Ready(Some(if observed.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(7_u8)
        } else {
            Err(
                Error::new(ErrorKind::PermissionDenied, "source_read", "original error")
                    .with_source(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "original cause",
                    )),
            )
        }))
    });
    let mut reader: Box<dyn Reader<Item = u8, Metadata = ()>> =
        Box::new(StreamReader::new(stream, ()));
    assert_eq!(reader.next().await.unwrap(), Some(7));
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "source_read");
    assert_eq!(error.to_string(), "source_read: original error");
    let cause = std::error::Error::source(&error)
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
    assert_eq!(cause.to_string(), "original cause");
    assert!(dropped.load(Ordering::SeqCst));
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dropping_stream_reader_releases_unpolled_or_partially_consumed_streams() {
    for consume in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropProbe::new(dropped.clone());
        let stream = stream::poll_fn(move |_| {
            let _keep_alive = &guard;
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(1)))
        });
        let mut reader = StreamReader::new(stream, ());
        if consume {
            assert_eq!(reader.next().await.unwrap(), Some(1));
        }
        assert!(!dropped.load(Ordering::SeqCst));
        drop(reader);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(consume));
    }
}
