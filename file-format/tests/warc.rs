use open_arrow_access_test_support::record::field_names;
use std::{error::Error as _, num::NonZeroUsize, sync::atomic::Ordering};

use arrow_array::{cast::AsArray, types::TimestampMillisecondType};
use futures::TryStreamExt;
use more_asserts::assert_lt;
use open_arrow_access_core::{
    Accessor, AccessorExt, ByteReadOptions, ByteReadRequest, Error, ErrorKind, ReadOptions,
    ReadRequest, ScanPlanner,
};
use open_arrow_access_file_format::DecodePartitionOptions;
use open_arrow_access_file_format::warc::{WarcAccessor, WarcArrowAccessor, WarcHeader};
use open_arrow_access_layers::ParallelReadLayer;
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
    for projection in [None, Some(vec!["body".into(), "id".into()]), Some(vec![])] {
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
                    batch["id"]
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
async fn parallel_record_partitions_are_lazy_and_preserve_binary_projection_and_order() {
    let bytes = fixture!("warc", "good_physical_records.warc").await;
    let input = MemoryAccessor::new(&bytes, 1);
    let observed = input.observations.clone();
    let accessor = WarcArrowAccessor::new(input).with_partition_options(DecodePartitionOptions {
        target_bytes: NonZeroUsize::MIN,
    });
    let request = ReadRequest::new("physical.warc").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: Some(vec!["body".into(), "id".into(), "date".into()]),
    });
    let mut plan = accessor.plan_scan(request.clone()).await.unwrap();
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    let partition = plan.partitions.try_next().await.unwrap().unwrap();
    assert_lt!(observed.reads.load(Ordering::SeqCst), bytes.len());
    let reads = observed.reads.load(Ordering::SeqCst);
    let mut first = partition.await.unwrap();
    assert_eq!(first.metadata().schema, plan.metadata.schema);
    assert_eq!(first.metadata().source, plan.metadata.source);
    assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(observed.reads.load(Ordering::SeqCst), reads);
    drop(plan);

    let mut reader = accessor
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(request)
        .await
        .unwrap();
    let mut ids = Vec::new();
    let mut bodies = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        // Every record exceeds the soft byte target and must still be accepted.
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch["date"]
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            1_594_176_775_000,
        );
        assert_eq!(batch.schema().field(0).name(), "body");
        ids.push(batch["id"].as_string::<i32>().value(0).to_owned());
        bodies.push(batch["body"].as_binary::<i64>().value(0).to_vec());
    }
    assert_eq!(
        ids,
        ["<urn:test:first>", "<urn:test:second>", "<urn:test:third>"]
    );
    assert_eq!(bodies, [vec![], vec![0, 255, 13, 10], b"end".to_vec()]);
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_arrow_projections_fail_before_opening_or_planning_input() {
    for projection in [vec!["missing".into()], vec!["body".into(), "body".into()]] {
        let input = MemoryAccessor::new(b"not a WARC file", 1);
        let observed = input.observations.clone();
        let accessor = WarcArrowAccessor::new(input);
        let request = ReadRequest::new("unused.warc").with_options(ReadOptions {
            projection: Some(projection),
            ..Default::default()
        });
        assert_eq!(
            accessor.open(request.clone()).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
        );
        assert_eq!(
            accessor.plan_scan(request).await.err().unwrap().kind(),
            ErrorKind::InvalidInput,
        );
        assert!(observed.requests.lock().unwrap().is_empty());
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn five_metadata_columns_preserve_order_and_optional_header_nulls_without_body() {
    let input = fixture!("warc", "good_physical_records.warc").await;
    let fields = ["target_uri", "type", "content_type", "id", "date"];
    let mut reader = WarcArrowAccessor::new(MemoryAccessor::new(&input, 1))
        .open(ReadRequest::new("records.warc").with_options(ReadOptions {
            projection: Some(fields.iter().map(|name| (*name).into()).collect()),
            ..Default::default()
        }))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch["date"]
            .as_primitive::<TimestampMillisecondType>()
            .values()
            .as_ref(),
        &[1_594_176_775_000; 3],
    );
    assert_eq!(field_names(batch.schema_ref()), fields);
    assert_eq!(
        batch["target_uri"]
            .as_string::<i64>()
            .iter()
            .collect::<Vec<_>>(),
        [None, Some("https://example.com/"), None]
    );
    assert_eq!(
        batch["content_type"]
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [None, Some("application/octet-stream"), None]
    );
    assert_eq!(
        batch["type"].as_string::<i32>().iter().collect::<Vec<_>>(),
        [Some("warcinfo"), Some("resource"), Some("continuation")]
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn empty_parallel_projection_preserves_rows_and_validates_unselected_bodies() {
    for (fixture_name, expected_rows) in [
        ("good_physical_records.warc", Some(3)),
        ("bad_truncated_body.warc", None),
    ] {
        let input = fixture!("warc", fixture_name).await;
        let mut reader = WarcArrowAccessor::new(MemoryAccessor::new(&input, 1))
            .with_partition_options(DecodePartitionOptions {
                target_bytes: NonZeroUsize::MIN,
            })
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(ReadRequest::new(fixture_name).with_options(ReadOptions {
                projection: Some(vec![]),
                ..Default::default()
            }))
            .await
            .unwrap();
        if let Some(expected_rows) = expected_rows {
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch.num_columns(), 0);
                rows += batch.num_rows();
            }
            assert_eq!(rows, expected_rows);
        } else {
            assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn standard_headers_survive_sequential_parallel_and_positioned_projection() {
    use open_arrow_access_core::{FactoryAccessor, RecordReader};
    use open_arrow_access_file_format::warc::WarcArrowFormat;
    use open_arrow_access_layers::SourceColumns;

    let first = concat!(
        "WARC/1.1\r\n",
        "WARC-Type: resource\r\n",
        "WARC-Record-ID: <urn:test:headers>\r\n",
        "WARC-Date: 2020-07-08T02:52:55Z\r\n",
        "Content-Length: 3\r\n",
        "WARC-Block-Digest: sha1:block\r\n",
        "WARC-Payload-Digest: sha1:payload\r\n",
        "WARC-Concurrent-To: <urn:test:concurrent>\r\n",
        "WARC-Filename: example.warc\r\n",
        "WARC-Identified-Payload-Type: text/plain\r\n",
        "WARC-IP-Address: 2001:db8::1\r\n",
        "WARC-Profile: https://example.com/profile\r\n",
        "WARC-Refers-To: <urn:test:original>\r\n",
        "WARC-Segment-Number: 2\r\n",
        "WARC-Segment-Origin-ID: <urn:test:origin>\r\n",
        "WARC-Segment-Total-Length: 12345\r\n",
        "WARC-Truncated: length\r\n",
        "WARC-Warcinfo-ID: <urn:test:info>\r\n",
        "\r\nabc\r\n\r\n",
    );
    let second = concat!(
        "WARC/1.1\r\n",
        "WARC-Type: warcinfo\r\n",
        "WARC-Record-ID: <urn:test:empty>\r\n",
        "WARC-Date: 2020-07-08T02:52:55Z\r\n",
        "Content-Length: 0\r\n\r\n\r\n\r\n",
    );
    let input = [first, second].concat();
    let headers = [
        ("content_length", "3"),
        ("block_digest", "sha1:block"),
        ("payload_digest", "sha1:payload"),
        ("concurrent_to", "<urn:test:concurrent>"),
        ("filename", "example.warc"),
        ("identified_payload_type", "text/plain"),
        ("ip_address", "2001:db8::1"),
        ("profile", "https://example.com/profile"),
        ("refers_to", "<urn:test:original>"),
        ("segment_number", "2"),
        ("segment_origin_id", "<urn:test:origin>"),
        ("segment_total_length", "12345"),
        ("truncated", "length"),
        ("warc_info_id", "<urn:test:info>"),
    ];
    for projection in [
        None,
        Some(
            headers
                .iter()
                .rev()
                .map(|(name, _)| (*name).into())
                .collect(),
        ),
        Some(Vec::new()),
    ] {
        let request = ReadRequest::new("headers.warc").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: projection.clone(),
        });
        let sequential = WarcArrowAccessor::new(MemoryAccessor::new(input.as_bytes(), 7))
            .open(request.clone())
            .await
            .unwrap();
        let parallel = WarcArrowAccessor::new(MemoryAccessor::new(input.as_bytes(), 7))
            .with_partition_options(DecodePartitionOptions {
                target_bytes: NonZeroUsize::MIN,
            })
            .layer(ParallelReadLayer::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::MIN,
            ))
            .open(request.clone())
            .await
            .unwrap();
        let positioned = SourceColumns::new()
            .wrap(FactoryAccessor::new(
                MemoryAccessor::new(input.as_bytes(), 7),
                WarcArrowFormat.with_source_positions(),
            ))
            .open(request)
            .await
            .unwrap();
        let readers: [Box<dyn RecordReader>; 3] = [sequential, parallel, positioned];
        for mut reader in readers {
            let schema = reader.metadata().schema.clone();
            assert_eq!(
                schema.fields().len(),
                projection.as_ref().map_or(22, Vec::len)
            );
            if let Some(names) = &projection {
                assert_eq!(
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                    names.iter().collect::<Vec<_>>()
                );
            }
            if projection.is_none() {
                assert_eq!(
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>(),
                    [
                        "id",
                        "type",
                        "warc_version",
                        "date",
                        "source_uri",
                        "target_uri",
                        "content_type",
                        "body",
                        "content_length",
                        "block_digest",
                        "payload_digest",
                        "concurrent_to",
                        "filename",
                        "identified_payload_type",
                        "ip_address",
                        "profile",
                        "refers_to",
                        "segment_number",
                        "segment_origin_id",
                        "segment_total_length",
                        "truncated",
                        "warc_info_id",
                    ]
                );
                let target = schema.field_with_name("target_uri").unwrap();
                assert_eq!(target.data_type(), &arrow_schema::DataType::LargeUtf8);
                assert!(target.is_nullable());
            }
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                assert_eq!(batch.schema(), schema);
                for (name, value) in headers {
                    if let Some(column) = batch.column_by_name(name) {
                        let field = schema.field_with_name(name).unwrap();
                        assert_eq!(field.data_type(), &arrow_schema::DataType::Utf8);
                        assert_eq!(field.is_nullable(), name != "content_length");
                        assert_eq!(
                            column.as_string::<i32>().iter().collect::<Vec<_>>(),
                            [if rows == 0 {
                                Some(value)
                            } else if name == "content_length" {
                                Some("0")
                            } else {
                                None
                            }],
                            "{name}",
                        );
                    }
                }
                rows += batch.num_rows();
            }
            assert_eq!(rows, 2);
        }
    }
}
