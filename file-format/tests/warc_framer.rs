#![cfg(feature = "warc")]

use futures::{StreamExt, TryStreamExt, io::Cursor};
use open_arrow_access_core::{Accessor, BytePosition, ErrorKind, bytes_reader};
use open_arrow_access_file_format::warc::WarcFramer;
use open_arrow_access_layers::{SourceFramer, SourceUnit};
use open_arrow_access_test_support::MemoryAccessor;

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

async fn frame(input: &[u8], chunk: usize) -> Vec<SourceUnit> {
    WarcFramer
        .frame(
            MemoryAccessor::new(input, chunk)
                .open("data.warc".into())
                .await
                .unwrap(),
        )
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn units_preserve_binary_bodies_case_insensitive_headers_and_coordinates() {
    let body = b"\0\xff\r\nWARC/1.1\r\n";
    let record = record(body);
    let padding = b" \t\r\n";
    let mut input = padding.to_vec();
    let mut positions = Vec::new();
    for _ in 0..5 {
        positions.push(BytePosition::Stored(input.len() as u64));
        input.extend_from_slice(&record);
        input.extend_from_slice(padding);
    }
    for chunk in [1, 7, input.len()] {
        let units = frame(&input, chunk).await;
        assert_eq!(
            units.iter().map(|u| u.positions.len()).collect::<Vec<_>>(),
            [1, 1, 1, 1, 1]
        );
        assert_eq!(
            units
                .iter()
                .flat_map(|u| u.positions.iter().copied())
                .collect::<Vec<_>>(),
            positions
        );
        for unit in units {
            let mut parsed = warc::WarcReader::new(Cursor::new(unit.bytes)).iter_records();
            for _ in &unit.positions {
                let decoded = parsed.next().await.unwrap().unwrap();
                assert_eq!(decoded.body(), body);
                assert_eq!(decoded.warc_id(), "<urn:test:0>");
            }
            assert!(parsed.next().await.is_none());
        }
    }
}

#[tokio::test]
async fn empty_and_whitespace_only_inputs_emit_no_units() {
    for input in [b"".as_slice(), b" \t\r\n\r\n"] {
        assert!(frame(input, 1).await.is_empty());
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
        let reader = MemoryAccessor::new(&input, 8192)
            .open("bad.warc".into())
            .await
            .unwrap();
        let mut units = WarcFramer.frame(reader);
        let error = units.next().await.unwrap().err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(units.next().await.is_none());
    }
}

#[tokio::test]
async fn unpositioned_input_returns_unsupported() {
    let reader = Box::new(bytes_reader(record(b"body").into(), None));
    let error = WarcFramer
        .frame(reader)
        .next()
        .await
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[tokio::test]
async fn large_record_bodies_are_preserved() {
    let body = vec![b'x'; 128 * 1024];
    let large = record(&body);
    let small = record(b"tail");
    let input = [large.as_slice(), small.as_slice()].concat();
    let units = frame(&input, 4096).await;
    assert_eq!(units.len(), 2);
    assert_eq!(units[0].positions, [BytePosition::Stored(0)]);
    assert_eq!(
        units[1].positions,
        [BytePosition::Stored(large.len() as u64)]
    );
    let mut records = warc::WarcReader::new(Cursor::new(units[0].bytes.clone())).iter_records();
    assert_eq!(records.next().await.unwrap().unwrap().body(), body);
    assert!(records.next().await.is_none());
}

#[tokio::test]
async fn storage_failures_retain_their_category() {
    let input = record(b"body");
    for offset in [0, 10, input.len() - 5] {
        let reader = MemoryAccessor::new(&input, 1)
            .with_failure(offset, ErrorKind::RateLimited)
            .open("data.warc".into())
            .await
            .unwrap();
        let mut units = WarcFramer.frame(reader);
        assert_eq!(
            units.next().await.unwrap().err().unwrap().kind(),
            ErrorKind::RateLimited
        );
        assert!(units.next().await.is_none());
    }
}
