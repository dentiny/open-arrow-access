use open_arrow_access_layers::ParallelReadLayer;
use std::{error::Error as _, num::NonZeroUsize, sync::atomic::Ordering};

use arrow_array::cast::AsArray;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, Error, ErrorKind, ReadOptions,
    ReadRequest, ScanPlanner,
};
use open_arrow_access_file_format::warc::{WarcAccessor, WarcArrowAccessor, WarcHeader};
use open_arrow_access_test_support::{MemoryAccessor, fixture, reader};

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
        reader::assert_finished(reader.as_mut(), &observed.reads, Clone::clone).await;
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
    let options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(3).unwrap(),
        io_chunk_size: NonZeroUsize::new(5),
    };
    reader::assert_error_contract(
        WarcAccessor::new,
        &bytes,
        ByteReadRequest::new("a.warc").with_options(options),
        // Header, body, and the terminator parser after the body.
        &[0, body_start + 1, bytes.len() - 1],
        Clone::clone,
        assert_source_error,
    )
    .await;
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
    reader::assert_finished(reader.as_mut(), &observed.reads, Clone::clone).await;
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
async fn arrow_records_preserve_binary_bodies_source_and_projection() {
    let input = fixture!("warc", "good_physical_records.warc").await;
    for projection in [
        None,
        Some(vec!["body".into(), "record_id".into()]),
        Some(vec![]),
    ] {
        let mut reader = WarcArrowAccessor::new(MemoryAccessor::new(&input, 1))
            .open(ReadRequest::new("records.warc").with_options(ReadOptions {
                batch_size: NonZeroUsize::new(2).unwrap(),
                projection: projection.clone(),
            }))
            .await
            .unwrap();
        let mut bodies = Vec::new();
        let mut ids = Vec::new();
        let mut rows = 0;
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), reader.metadata().schema);
            rows += batch.num_rows();
            if let Some(body) = batch.column_by_name("body") {
                bodies.extend(
                    body.as_binary::<i64>()
                        .iter()
                        .map(|value| value.unwrap().to_vec()),
                );
                ids.extend(
                    batch["record_id"]
                        .as_string::<i32>()
                        .iter()
                        .map(|value| value.unwrap().to_owned()),
                );
            }
            if let Some(source) = batch.column_by_name("source_uri") {
                assert!(
                    source
                        .as_string::<i64>()
                        .iter()
                        .all(|value| value == Some("records.warc"))
                );
            }
        }
        assert_eq!(rows, 3);
        if projection.as_ref().is_none_or(|fields| !fields.is_empty()) {
            assert_eq!(bodies, [vec![], vec![0, 255, 13, 10], b"end".to_vec()]);
            assert_eq!(
                ids,
                ["<urn:test:first>", "<urn:test:second>", "<urn:test:third>"]
            );
        }
    }
}

#[tokio::test]
async fn arrow_records_propagate_parser_errors() {
    let bytes = fixture!("warc", "bad_truncated_body.warc").await;
    let mut reader = WarcArrowAccessor::new(MemoryAccessor::new(&bytes, 1))
        .open(ReadRequest::new("bad.warc"))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn physical_warc_plan_keeps_one_ordered_partition() {
    let bytes = fixture!("warc", "good_physical_records.warc").await;
    let accessor = WarcArrowAccessor::new(MemoryAccessor::new(&bytes, 1));
    let request = ReadRequest::new("records.warc");
    let plan = accessor.plan_scan(request.clone()).await.unwrap();
    assert_eq!(plan.partitions.size_hint().0, 1);
    let mut expected = accessor.open(request.clone()).await.unwrap();
    let mut actual = accessor
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request)
        .await
        .unwrap();
    loop {
        let batch = expected.next().await.unwrap();
        assert_eq!(actual.next().await.unwrap(), batch);
        if batch.is_none() {
            break;
        }
    }
}
