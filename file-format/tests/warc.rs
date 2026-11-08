use std::{error::Error as _, num::NonZeroUsize, sync::atomic::Ordering};

use open_arrow_access_core::{Accessor, ByteReadOptions, ByteReadRequest, Error, ErrorKind};
use open_arrow_access_file_format::warc::{WarcAccessor, WarcHeader};
use open_arrow_access_storage::OpenDalAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture, fs_operator};

#[tokio::test]
async fn parser_preserves_physical_records_order_headers_and_owned_binary_bodies() {
    let input = fixture!("warc", "good_physical_records.warc").await;
    let accessor = WarcAccessor::new(MemoryAccessor::new(&input, 1));
    let mut reader = accessor
        .open(ByteReadRequest::new("good_physical_records.warc"))
        .await
        .unwrap();
    assert_eq!(reader.metadata().locator(), "good_physical_records.warc");
    assert!(!reader.capability().read_with_range);
    assert!(!reader.capability().read_parallel);
    assert!(!reader.capability().list);
    let mut records = Vec::new();
    while let Some(record) = reader.next().await.unwrap() {
        records.push(record);
    }
    drop(reader);
    assert_eq!(
        records.iter().map(|r| r.warc_id()).collect::<Vec<_>>(),
        ["<urn:test:first>", "<urn:test:second>", "<urn:test:third>"]
    );
    assert_eq!(
        records
            .iter()
            .map(|r| r.warc_type().as_str())
            .collect::<Vec<_>>(),
        ["warcinfo", "resource", "continuation"]
    );
    assert_eq!(records[0].body(), b"");
    assert_eq!(records[1].body(), &[0, 255, 13, 10]);
    assert_eq!(records[2].body(), b"end");
    assert!(records[0].header(WarcHeader::ContentType).is_none());
    assert_eq!(
        records[1].header(WarcHeader::ContentType).as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(
        records[2].header(WarcHeader::SegmentNumber).as_deref(),
        Some("2")
    );
}

#[tokio::test]
async fn request_and_response_records_are_both_delivered() {
    let input = MemoryAccessor::new(&fixture!("warc", "good_request_response.warc").await, 1);
    let mut reader = WarcAccessor::new(input)
        .open("a.warc".into())
        .await
        .unwrap();
    let response = reader.next().await.unwrap().unwrap();
    let request = reader.next().await.unwrap().unwrap();
    assert_eq!(response.warc_type().as_str(), "response");
    assert_eq!(request.warc_type().as_str(), "request");
    assert_eq!(response.body(), b"x");
    assert_eq!(request.body(), b"y");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn malformed_and_truncated_records_fail_terminally() {
    for name in [
        "bad_incomplete_header.warc",
        "bad_truncated_body.warc",
        "bad_terminator.warc",
        "bad_missing_date.warc",
    ] {
        let input = MemoryAccessor::new(&fixture!("warc", name).await, 1);
        let observed = input.observations.clone();
        let mut reader = WarcAccessor::new(input).open(name.into()).await.unwrap();
        let error = reader.next().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode, "{name}");
        assert_eq!(error.operation(), "decode_warc");
        assert!(error.source().is_some());
        let polls = observed.reads.load(Ordering::SeqCst);
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(observed.reads.load(Ordering::SeqCst), polls);
    }
}

fn assert_source_error(error: &Error, kind: ErrorKind) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "decode_warc");
    let parser_error = error.source().unwrap();
    assert_eq!(ErrorKind::from_source(parser_error), Some(kind));
    let bridge_error = parser_error
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    let original = bridge_error
        .get_ref()
        .unwrap()
        .downcast_ref::<Error>()
        .unwrap();
    assert_eq!(original.operation(), "memory_read");
    assert_eq!(
        original
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::ConnectionReset
    );
}

#[tokio::test]
async fn source_errors_keep_categories_and_chains_across_parser_boundaries() {
    let bytes = fixture!("warc", "good_resource.warc").await;
    let body_start = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Unsupported,
        ErrorKind::InvalidInput,
        ErrorKind::Decode,
    ] {
        for offset in [0, body_start + 1, bytes.len() - 1] {
            // The final offset exercises the terminator parser after the body.
            let input = MemoryAccessor::new(&bytes, 1).with_failure(offset, kind);
            let observed = input.observations.clone();
            let mut reader = WarcAccessor::new(input)
                .open("a.warc".into())
                .await
                .unwrap();
            let error = reader.next().await.err().unwrap();
            assert_source_error(&error, kind);
            let polls = observed.reads.load(Ordering::SeqCst);
            assert!(reader.next().await.unwrap().is_none());
            assert_eq!(observed.reads.load(Ordering::SeqCst), polls);
        }
    }
}

#[tokio::test]
async fn earlier_record_remains_delivered_when_the_next_record_fails() {
    let boundary = fixture!("warc", "good_resource.warc").await.len();
    let input = MemoryAccessor::new(&fixture!("warc", "good_resources.warc").await, 1)
        .with_failure(boundary, ErrorKind::PermissionDenied);
    let mut reader = WarcAccessor::new(input)
        .open("a.warc".into())
        .await
        .unwrap();
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.warc_id(), "<urn:test:a>");
    assert_source_error(
        &reader.next().await.err().unwrap(),
        ErrorKind::PermissionDenied,
    );
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(first.body(), b"123");
}

#[tokio::test]
async fn reads_one_record_on_demand_forwards_io_options_and_stops_on_drop() {
    let boundary = fixture!("warc", "good_resource.warc").await.len();
    let input = MemoryAccessor::new(&fixture!("warc", "good_resources.warc").await, 1);
    let observed = input.observations.clone();
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(5),
    };
    let mut reader = WarcAccessor::new(input)
        .open(ByteReadRequest::new("a.warc").with_options(options))
        .await
        .unwrap();
    assert_eq!(observed.requests.lock().unwrap()[0].options, options);
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), boundary);
    drop(reader);
    assert_eq!(observed.reads.load(Ordering::SeqCst), boundary);
    assert_eq!(first.warc_id(), "<urn:test:a>");
}

#[tokio::test]
async fn empty_input_and_independent_opens_have_terminal_eof() {
    let empty = MemoryAccessor::new(&fixture!("warc", "empty.warc").await, 1);
    let observed = empty.observations.clone();
    let mut reader = WarcAccessor::new(empty)
        .open("empty.warc".into())
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_none());
    let polls = observed.reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(observed.reads.load(Ordering::SeqCst), polls);
    let input = MemoryAccessor::new(&fixture!("warc", "good_metadata.warc").await, 2);
    let accessor = WarcAccessor::new(input);
    let mut left = accessor.open("a.warc".into()).await.unwrap();
    let mut right = accessor.open("a.warc".into()).await.unwrap();
    assert_eq!(
        left.next().await.unwrap().unwrap(),
        right.next().await.unwrap().unwrap()
    );
    assert!(left.next().await.unwrap().is_none());
    assert!(right.next().await.unwrap().is_none());
}

#[tokio::test]
async fn open_errors_are_forwarded_and_empty_chunks_are_terminal_io_errors() {
    let dir = tempfile::tempdir().unwrap();
    let accessor = WarcAccessor::new(OpenDalAccessor::new(fs_operator(dir.path())));
    let error = accessor.open("missing.warc".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(error.source().is_some());
    let input =
        MemoryAccessor::new(&fixture!("warc", "good_resource.warc").await, 1).with_empty_chunk(0);
    let observed = input.observations.clone();
    let mut reader = WarcAccessor::new(input)
        .open("a.warc".into())
        .await
        .unwrap();
    let error = reader.next().await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::Io);
    let polls = observed.reads.load(Ordering::SeqCst);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(observed.reads.load(Ordering::SeqCst), polls);
}
