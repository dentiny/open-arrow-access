use super::{NamespaceBinding, XmlAccessor, XmlField, XmlLimits, XmlSchema};
use arrow_array::{Array, Int64Array, ListArray, StringArray};
use arrow_schema::DataType::{Int64, Utf8};
use open_arrow_access_core::{Accessor, ByteReadOptions, ErrorKind, ReadOptions, ReadRequest};
use open_arrow_access_test_support::MemoryAccessor;
use std::{
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
};

fn schema() -> XmlSchema {
    XmlSchema::with_namespaces(
        vec![
            XmlField::new("id", "@m:id", Int64, false),
            XmlField::new("title", "b:title", Utf8, true),
            XmlField::list(
                "tags",
                "b:tags",
                true,
                XmlField::new("item", "b:tag", Utf8, false),
            ),
        ],
        &[
            NamespaceBinding {
                prefix: "b",
                uri: "urn:catalog",
            },
            NamespaceBinding {
                prefix: "m",
                uri: "urn:meta",
            },
        ],
    )
    .unwrap()
}

fn options(projection: Option<Vec<&str>>) -> ReadOptions {
    ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        projection: projection.map(|names| names.into_iter().map(String::from).collect()),
    }
}

#[tokio::test]
async fn fragmented_xml_preserves_namespaces_nested_values_schema_and_order() {
    let input = open_arrow_access_test_support::fixture!("xml", "catalog.xml").await;
    let bytes = MemoryAccessor::new(&input, 1).with_ranges().with_listing();
    let observations = bytes.observations.clone();
    let accessor = XmlAccessor::new(bytes, schema(), "/b:catalog/b:book");
    let capability = accessor.capability();
    assert!(capability.read);
    assert!(!capability.read_with_range && !capability.read_parallel && !capability.list);
    let request = ReadRequest::new("catalog.xml")
        .with_options(options(None))
        .with_byte_options(ByteReadOptions {
            io_concurrency: NonZeroUsize::new(3).unwrap(),
            io_chunk_size: NonZeroUsize::new(7),
        });
    let mut reader = accessor.open(request.clone()).await.unwrap();
    assert_eq!(observations.reads.load(Ordering::SeqCst), 0);
    assert_eq!(reader.metadata().schema, schema().arrow_schema());
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "catalog.xml"
    );
    assert!(
        !reader.capability().read_with_range
            && !reader.capability().read_parallel
            && !reader.capability().list
    );
    assert_eq!(
        observations.requests.lock().unwrap()[0].options,
        request.byte_options
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), reader.metadata().schema);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2]
    );
    let titles = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(titles.value(0), "Rust & 箭头");
    assert!(titles.is_null(1));
    let tags = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(tags.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        tags.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "<XML>"
    );
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        ""
    );
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.next().await.unwrap().is_none());
    let mut independent = accessor.open(request).await.unwrap();
    assert_eq!(independent.next().await.unwrap().unwrap().num_rows(), 2);
}

#[tokio::test]
async fn projection_reorders_skips_conversion_and_counts_zero_columns() {
    let schema = XmlSchema::new(vec![
        XmlField::new("bad", "bad", Int64, false),
        XmlField::new("name", "@name", Utf8, false),
        XmlField::new("id", "@id", Int64, false),
    ])
    .unwrap();
    let bytes = MemoryAccessor::new(b"<root><row name='a' id='1'><bad>invalid</bad></row><row name='b' id='2'/><row name='c' id='3'/></root>", 3);
    let accessor = XmlAccessor::new(bytes, schema, "/root/row");
    let mut reader = accessor
        .open(ReadRequest::new("rows.xml").with_options(options(Some(vec!["id", "name"]))))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.field(0).name(), "id");
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2]
    );
    let mut reader = accessor
        .open(ReadRequest::new("rows.xml").with_options(options(Some(vec![]))))
        .await
        .unwrap();
    let mut rows = 0;
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 0);
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn invalid_configuration_fails_before_opening_input() {
    let bytes = MemoryAccessor::new(b"<root/>", 1);
    let observations = bytes.observations.clone();
    for (path, limits) in [
        ("relative/path", XmlLimits::default()),
        ("/unbound:root", XmlLimits::default()),
        (
            "/root",
            XmlLimits {
                max_record_bytes: 50,
                max_batch_bytes: 49,
                max_depth: 1,
            },
        ),
    ] {
        let accessor = XmlAccessor::new(bytes.clone(), schema(), path).with_limits(limits);
        let error = accessor
            .open(ReadRequest::new("input"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    let accessor = XmlAccessor::new(bytes, schema(), "/b:catalog/b:book");
    for names in [vec!["missing"], vec!["id", "id"]] {
        let error = accessor
            .open(ReadRequest::new("input").with_options(options(Some(names))))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
    assert!(observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_input_limits_and_source_failures_are_terminal() {
    let mapping = XmlSchema::new(vec![XmlField::new("id", "@id", Int64, false)]).unwrap();
    for input in [
        b"<root><row id='oops'/></root>".as_slice(),
        b"<root><row id='1'/>",
        b"<root><skip>&unknown;</skip></root>",
        b"<!DOCTYPE root><root/>",
    ] {
        let accessor =
            XmlAccessor::new(MemoryAccessor::new(input, 1), mapping.clone(), "/root/row");
        let mut reader = accessor.open(ReadRequest::new("bad.xml")).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert!(std::error::Error::source(&error).is_some());
        assert!(reader.next().await.unwrap().is_none());
    }
    for (bytes, expected) in [
        (
            MemoryAccessor::new(b"<root><row id='1'/></root>", 1)
                .with_failure(14, ErrorKind::PermissionDenied),
            ErrorKind::PermissionDenied,
        ),
        (
            MemoryAccessor::new(b"<root/>", 1).with_empty_chunk(2),
            ErrorKind::Io,
        ),
    ] {
        let accessor = XmlAccessor::new(bytes, mapping.clone(), "/root/row");
        let mut reader = accessor.open(ReadRequest::new("bad.xml")).await.unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(reader.next().await.unwrap().is_none());
    }
    for input in [
        "<root><skip><deep/></skip></root>".to_owned(),
        format!("<root><!--{}--></root>", "x".repeat(1000)),
    ] {
        let accessor = XmlAccessor::new(
            MemoryAccessor::new(input.as_bytes(), 1),
            mapping.clone(),
            "/root/row",
        )
        .with_limits(XmlLimits {
            max_record_bytes: 32,
            max_batch_bytes: 64,
            max_depth: 2,
        });
        let mut reader = accessor.open(ReadRequest::new("large.xml")).await.unwrap();
        assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn demand_and_drop_stop_reading_without_detached_work() {
    let xml = format!(
        "<root><row id='1'/><skip>{}</skip></root>",
        "x".repeat(100_000)
    );
    let bytes = MemoryAccessor::new(xml.as_bytes(), 1);
    let observations = bytes.observations.clone();
    let weak = Arc::downgrade(&observations);
    let schema = XmlSchema::new(vec![XmlField::new("id", "@id", Int64, false)]).unwrap();
    let accessor = XmlAccessor::new(bytes, schema, "/root/row");
    let mut reader = accessor
        .open(ReadRequest::new("large.xml").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            projection: None,
        }))
        .await
        .unwrap();
    drop(accessor);
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    let reads = observations.reads.load(Ordering::SeqCst);
    assert!(reads < 100);
    drop(reader);
    assert_eq!(observations.reads.load(Ordering::SeqCst), reads);
    drop(observations);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn batch_byte_limits_preserve_lookahead_and_row_order() {
    let schema = XmlSchema::new(vec![XmlField::new("id", "@id", Int64, false)]).unwrap();
    let accessor = XmlAccessor::new(
        MemoryAccessor::new(b"<root><row id='1'/><row id='2'/><row id='3'/></root>", 1),
        schema,
        "/root/row",
    )
    .with_limits(XmlLimits {
        max_record_bytes: 13,
        max_batch_bytes: 26,
        max_depth: 2,
    });
    let mut reader = accessor
        .open(ReadRequest::new("limited.xml"))
        .await
        .unwrap();
    let first = reader.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    let second = reader.next().await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert_eq!(
        second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert!(reader.next().await.unwrap().is_none());
}
