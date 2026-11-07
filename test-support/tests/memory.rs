use std::{error::Error as _, num::NonZeroUsize, sync::atomic::Ordering};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Capability, ErrorKind,
};
use open_arrow_access_test_support::MemoryAccessor;

const DATA: &[u8] = include_bytes!("../../testdata/csv/good_no_header.csv");

#[tokio::test]
async fn readers_have_independent_cursors_and_ranges_do_not_advance_them() {
    let input = MemoryAccessor::new(DATA, 2)
        .with_ranges()
        .with_listing()
        .with_version("v1");
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(7),
    };
    let mut first = input
        .open(ByteReadRequest::new("first").with_options(options))
        .await
        .unwrap();
    let mut second = input.clone().open("second".into()).await.unwrap();
    assert_eq!(
        input.capability(),
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: true
        }
    );
    assert_eq!(
        first.capability(),
        Capability {
            read: true,
            read_with_range: true,
            read_parallel: false,
            list: false
        }
    );
    assert_eq!(first.metadata().content_length, Some(DATA.len() as u64));
    assert_eq!(first.metadata().version.as_deref(), Some("v1"));
    assert_eq!(first.metadata().source.as_ref().unwrap().locator(), "first");
    assert_eq!(input.observations.reads.load(Ordering::SeqCst), 0);
    let requests = input.observations.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].options, options);
    assert_eq!(requests[1].object.locator(), "second");

    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), &DATA[..2]);
    let ranges = first.read_ranges(vec![2..4, 0..2, 2..2]).await.unwrap();
    assert_eq!(ranges[0].as_ref(), &DATA[2..4]);
    assert_eq!(ranges[1].as_ref(), &DATA[..2]);
    assert!(ranges[2].is_empty());
    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), &DATA[2..4]);
    assert_eq!(second.next().await.unwrap().unwrap().as_ref(), &DATA[..2]);
    for range in [
        std::ops::Range { start: 2, end: 1 },
        0..DATA.len() as u64 + 1,
    ] {
        assert_eq!(
            first.read_range(range).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(first.next().await.unwrap().unwrap().as_ref(), &DATA[4..6]);
    let listed = input
        .list("prefix".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(listed[0].locator(), "prefix");
}

#[tokio::test]
async fn sequential_defaults_and_eof_follow_the_contract() {
    let input = MemoryAccessor::new(DATA, usize::MAX);
    assert_eq!(
        input.capability(),
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false
        }
    );
    let mut reader = input.open("fixture.csv".into()).await.unwrap();
    assert_eq!(reader.capability(), input.capability());
    assert!(reader.metadata().version.is_none());
    assert_eq!(
        reader.read_range(0..1).await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(reader.read_ranges(vec![]).await.unwrap().is_empty());
    assert_eq!(
        input.list("prefix".into()).await.err().unwrap().kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), DATA);
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(input.observations.reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn faults_are_independent_per_reader_and_errors_are_terminal() {
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        for offset in [0, 2, DATA.len()] {
            let input = MemoryAccessor::new(DATA, 2).with_failure(offset, kind);
            for _ in 0..2 {
                let mut reader = input.open("fixture.csv".into()).await.unwrap();
                let error = loop {
                    match reader.next().await {
                        Ok(Some(_)) => {}
                        Ok(None) => panic!("expected injected failure"),
                        Err(error) => break error,
                    }
                };
                assert_eq!(error.kind(), kind);
                assert_eq!(error.operation(), "memory_read");
                assert_eq!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<std::io::Error>()
                        .unwrap()
                        .kind(),
                    std::io::ErrorKind::ConnectionReset
                );
                assert!(reader.next().await.unwrap().is_none());
                assert!(reader.next().await.unwrap().is_none());
            }
        }
    }
    for offset in [0, DATA.len()] {
        let input = MemoryAccessor::new(DATA, usize::MAX).with_empty_chunk(offset);
        for _ in 0..2 {
            let mut reader = input.open("fixture.csv".into()).await.unwrap();
            let mut output = Vec::new();
            let mut empty_chunks = 0;
            while let Some(chunk) = reader.next().await.unwrap() {
                if chunk.is_empty() {
                    empty_chunks += 1;
                }
                output.extend_from_slice(&chunk);
            }
            assert_eq!(empty_chunks, 1);
            assert_eq!(output, DATA);
            assert!(reader.next().await.unwrap().is_none());
        }
    }
}
