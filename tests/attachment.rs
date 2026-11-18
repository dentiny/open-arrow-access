use arrow_array::LargeBinaryArray;
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
use arrow_array::cast::AsArray;
use open_arrow_access::{
    Accessor, AttachmentAccessor, AttachmentFormat, AttachmentOptions, ByteReadOptions, ErrorKind,
    OrderRequirement, OuterCompression, ReadOptions, ReadRequest, RecordCapability,
    open_attachment_source,
};
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn default_binary_ignores_format_and_compression_suffixes() {
    let bytes = b"\0\xffraw bytes";
    let source = open_attachment_source(
        MemoryAccessor::new(bytes, 2),
        ReadRequest::new("opaque.json.gz"),
        AttachmentOptions::default(),
    )
    .await
    .unwrap();
    let mut reader = source.reader().await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch["data"]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        bytes
    );
}

#[tokio::test]
async fn invalid_configuration_and_disabled_formats_fail_before_byte_io() {
    let mut cases = vec![
        (
            AttachmentOptions::new(AttachmentFormat::Binary)
                .with_outer_compression(OuterCompression::Auto),
            ErrorKind::InvalidInput,
        ),
        (
            AttachmentOptions::new(AttachmentFormat::Xml {
                record_path: " ".into(),
            }),
            ErrorKind::InvalidInput,
        ),
    ];
    for (format, enabled) in [
        (AttachmentFormat::Csv, cfg!(feature = "format-csv")),
        (AttachmentFormat::Tsv, cfg!(feature = "format-csv")),
        (AttachmentFormat::Json, cfg!(feature = "format-json")),
        (AttachmentFormat::Jsonl, cfg!(feature = "format-json")),
        (AttachmentFormat::Parquet, cfg!(feature = "format-parquet")),
        (
            AttachmentFormat::Xml {
                record_path: "/rows/row".into(),
            },
            cfg!(feature = "format-xml"),
        ),
    ] {
        if !enabled {
            cases.push((AttachmentOptions::new(format), ErrorKind::Unsupported));
        }
    }
    for (options, expected) in cases {
        let input = MemoryAccessor::new(b"unread", 2);
        let observations = input.observations.clone();
        let constructor_error = AttachmentAccessor::new(input.clone(), options.clone())
            .err()
            .unwrap();
        assert_eq!(constructor_error.kind(), expected);
        assert!(observations.requests.lock().unwrap().is_empty());
        let error = open_attachment_source(input, ReadRequest::new("file"), options)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), expected);
        assert!(observations.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
async fn selected_text_format_dispatches_with_its_defaults() {
    let cases: Vec<(AttachmentFormat, &[u8])> = vec![
        #[cfg(feature = "format-csv")]
        (AttachmentFormat::Csv, b"label\na\n"),
        #[cfg(feature = "format-csv")]
        (AttachmentFormat::Tsv, b"id\tlabel\n1\ta\n"),
        #[cfg(feature = "format-json")]
        (AttachmentFormat::Json, b"{\"label\":\"a\"}"),
        #[cfg(feature = "format-json")]
        (AttachmentFormat::Jsonl, b"{\"label\":\"a\"}\n"),
        #[cfg(feature = "format-xml")]
        (
            AttachmentFormat::Xml {
                record_path: "/rows/row".into(),
            },
            b"<rows><row><label>a</label></row></rows>",
        ),
    ];
    for (format, bytes) in cases {
        let source = open_attachment_source(
            MemoryAccessor::new(bytes, 3),
            ReadRequest::new("opaque"),
            AttachmentOptions::new(format),
        )
        .await
        .unwrap();
        let mut reader = source.reader().await.unwrap();
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch["label"].as_string::<i32>().value(0), "a");
    }
}

#[tokio::test]
async fn reusable_binary_accessor_keeps_independent_readers_and_byte_options() {
    use std::num::NonZeroUsize;
    let bytes = b"\0\xffraw bytes";
    let input = MemoryAccessor::new(bytes, 2);
    let observations = input.observations.clone();
    let records = AttachmentAccessor::new(input, AttachmentOptions::default()).unwrap();
    let cloned = records.clone();
    assert!(observations.requests.lock().unwrap().is_empty());
    assert_eq!(
        records.capability(),
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    );
    let mut first = records.open(ReadRequest::new("first.bin")).await.unwrap();
    let mut second = cloned.open(ReadRequest::new("second.bin")).await.unwrap();
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: NonZeroUsize::new(64),
    };
    let mut count_only = records
        .open(
            ReadRequest::new("opaque.gz")
                .with_options(ReadOptions {
                    projection: Some(vec![]),
                    batch_size: NonZeroUsize::MIN,
                    ..ReadOptions::default()
                })
                .with_byte_options(byte_options),
        )
        .await
        .unwrap();
    drop(records);
    drop(cloned);
    for reader in [&mut first, &mut second] {
        let batch = reader.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch["data"]
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            bytes
        );
        assert!(reader.next().await.unwrap().is_none());
    }
    let batch = count_only.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 0);
    assert!(count_only.next().await.unwrap().is_none());
    let requests = observations.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].object.locator(), "first.bin");
    assert_eq!(requests[1].object.locator(), "second.bin");
    assert_eq!(requests[2].object.locator(), "opaque.gz");
    assert_eq!(requests[2].options, byte_options);
}

#[tokio::test]
#[cfg(feature = "format-csv")]
async fn reusable_csv_accessor_preserves_projection_batch_size_and_cursor_independence() {
    use std::num::NonZeroUsize;
    let input = MemoryAccessor::new(b"id,label\n1,alpha\n2,beta\n3,gamma\n", 4);
    let records =
        AttachmentAccessor::new(input, AttachmentOptions::new(AttachmentFormat::Csv)).unwrap();
    let request = ReadRequest::new("rows.csv").with_options(ReadOptions {
        projection: Some(vec!["label".into()]),
        batch_size: NonZeroUsize::MIN,
        ..ReadOptions::default()
    });
    let mut first = records.open(request.clone()).await.unwrap();
    let mut second = records.clone().open(request).await.unwrap();
    assert_eq!(first.metadata().schema.fields().len(), 1);
    assert_eq!(first.metadata().schema.field(0).name(), "label");
    for expected in ["alpha", "beta", "gamma"] {
        for reader in [&mut first, &mut second] {
            let batch = reader.next().await.unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch["label"].as_string::<i32>().value(0), expected);
        }
    }
    assert!(first.next().await.unwrap().is_none());
    assert!(second.next().await.unwrap().is_none());
}
