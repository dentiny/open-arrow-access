#![cfg(feature = "warc")]

use open_arrow_access_core::{
    BytePosition, ErrorKind, ReadOptions, ReadRequest, ReaderFactoryExt, bytes_reader,
};
use open_arrow_access_file_format::warc::WarcArrowFormat;
use open_arrow_access_test_support::MemoryAccessor;
use std::{num::NonZeroUsize, sync::atomic::Ordering};

fn record(body: &[u8]) -> Vec<u8> {
    let mut record = format!(
        "WARC/1.1\r\nwarc-type: resource\r\nwarc-record-id: <urn:test:0>\r\n\
         warc-date: 2020-07-08T02:52:55Z\r\ncontent-length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    record.extend_from_slice(body);
    record.extend_from_slice(b"\r\n\r\n");
    record
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_emit_no_batches() {
    for input in [b"".as_slice(), b" \t\r\n\r\n"] {
        let mut reader = WarcArrowFormat
            .with_source_positions()
            .open_reader(
                &MemoryAccessor::new(input, 1),
                ReadRequest::new("data.warc"),
            )
            .await
            .unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn malformed_records_and_header_limit_fail_terminally_with_decode() {
    let good = record(b"body");
    let mut bad_terminator = good.clone();
    *bad_terminator.last_mut().unwrap() = b'x';
    let mut long_header = b"WARC/1.1\r\nX-Long: ".to_vec();
    long_header.resize(4 * 1024 * 1024 + 1, b'x');
    long_header.extend_from_slice(b"\r\n\r\n\r\n\r\n");
    for input in [
        b"not WARC\r\n\r\n".to_vec(),
        b"WARC/1.1\r\nContent-Length: nope\r\n\r\n".to_vec(),
        good[..good.len() - 5].to_vec(),
        bad_terminator,
        long_header,
    ] {
        let mut reader = WarcArrowFormat
            .with_source_positions()
            .open_reader(
                &MemoryAccessor::new(&input, 8192),
                ReadRequest::new("bad.warc"),
            )
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(error.to_string().contains("bad.warc"));
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn unpositioned_input_returns_unsupported() {
    let input = Box::new(bytes_reader(record(b"body").into(), None));
    let mut reader = WarcArrowFormat
        .with_source_positions()
        .open_input(input, ReadRequest::new("data.warc"))
        .await
        .unwrap();
    assert_eq!(
        reader.next().await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn storage_failures_retain_their_category() {
    let input = record(b"body");
    for offset in [0, 10, input.len() - 5] {
        let mut reader = WarcArrowFormat
            .with_source_positions()
            .open_reader(
                &MemoryAccessor::new(&input, 1).with_failure(offset, ErrorKind::RateLimited),
                ReadRequest::new("data.warc"),
            )
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap_err().kind(),
            ErrorKind::RateLimited
        );
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn zero_column_projection_retains_positions_and_validation_without_read_ahead() {
    let first = record(&vec![b'x'; 128 * 1024]);
    let input = [first.as_slice(), record(b"tail").as_slice()].concat();
    let storage = MemoryAccessor::new(&input, 1);
    let observations = storage.observations.clone();
    let request = ReadRequest::new("data.warc").with_options(ReadOptions {
        batch_size: NonZeroUsize::MIN,
        projection: Some(Vec::new()),
    });
    let mut reader = WarcArrowFormat
        .with_source_positions()
        .open_reader(&storage, request)
        .await
        .unwrap();
    assert!(reader.metadata().schema.fields().is_empty());
    assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.batch().num_columns(), 0);
    assert_eq!(batch.batch().num_rows(), 1);
    assert_eq!(batch.positions(), [BytePosition::Stored(0)]);
    assert_eq!(observations.reads.load(Ordering::SeqCst), first.len());
    let reads = observations.reads.load(Ordering::SeqCst);
    drop(reader);
    assert_eq!(observations.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn positioned_decoding_matches_ordinary_batches_and_tracks_record_starts() {
    let padding = b" \t\r\n";
    let record = record(b"\0\xff\r\nWARC/1.1\r\n");
    let unpadded = record.repeat(5);
    let mut input = padding.to_vec();
    let mut positions = Vec::new();
    for _ in 0..5 {
        positions.push(BytePosition::Stored(input.len() as u64));
        input.extend_from_slice(&record);
        input.extend_from_slice(padding);
    }
    for chunk in [1, 7, input.len()] {
        for projection in [
            None,
            Some(vec!["date".into(), "body".into()]),
            Some(Vec::new()),
        ] {
            let request = ReadRequest::new("data.warc").with_options(ReadOptions {
                batch_size: NonZeroUsize::new(2).unwrap(),
                projection,
            });
            let storage = MemoryAccessor::new(&input, chunk);
            // The ordinary reader requires records without inter-record padding.
            let mut ordinary = WarcArrowFormat
                .open_reader(&MemoryAccessor::new(&unpadded, chunk), request.clone())
                .await
                .unwrap();
            let mut positioned = WarcArrowFormat
                .with_source_positions()
                .open_reader(&storage, request)
                .await
                .unwrap();
            assert_eq!(ordinary.metadata().schema, positioned.metadata().schema);
            for expected_positions in positions.chunks(2) {
                let expected = ordinary.next().await.unwrap().unwrap();
                let actual = positioned.next().await.unwrap().unwrap();
                assert_eq!(actual.positions(), expected_positions);
                assert_eq!(actual.batch().num_rows(), expected_positions.len());
                assert_eq!(actual.batch(), &expected);
            }
            assert!(ordinary.next().await.unwrap().is_none());
            assert!(positioned.next().await.unwrap().is_none());
        }
    }
}
