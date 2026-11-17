use arrow_array::LargeBinaryArray;
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
use arrow_array::cast::AsArray;
use open_arrow_access::{
    AttachmentFormat, AttachmentOptions, ErrorKind, OuterCompression, ReadRequest,
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

// Applications call this helper from Send futures, including async-trait
// Accessor::open implementations. Exercise that contract with every enabled
// format dispatch branch compiled into the helper.
#[tokio::test]
async fn attachment_binding_future_can_be_spawned() {
    let future = open_attachment_source(
        MemoryAccessor::new(b"payload", 2),
        ReadRequest::new("attachment.bin"),
        AttachmentOptions::default(),
    );
    let source = tokio::spawn(future).await.unwrap().unwrap();
    let mut reader = source.reader().await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());
}
