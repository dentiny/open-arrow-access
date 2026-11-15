#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
use std::num::NonZeroUsize;

use arrow_array::LargeBinaryArray;
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml",
    feature = "format-parquet"
))]
use arrow_array::cast::AsArray;
use open_arrow_access::{
    AttachmentFormat, AttachmentOptions, ErrorKind, OuterCompression, ReadOptions, ReadRequest,
    open_attachment_source,
};
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
use open_arrow_access::{BoundSource, ByteReadOptions};
use open_arrow_access_test_support::MemoryAccessor;

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn binary_preserves_raw_empty_invalid_utf8_and_compressed_bytes() {
    for bytes in [Vec::new(), vec![0, 255, 128, 10], gzip(b"original bytes")] {
        let input = MemoryAccessor::new(&bytes, 2);
        let observations = input.observations.clone();
        let source = open_attachment_source(
            input,
            ReadRequest::new("opaque.json.gz"),
            AttachmentOptions::default(),
        )
        .await
        .unwrap();
        assert!(
            observations.requests.lock().unwrap().is_empty(),
            "binary binding is lazy"
        );
        let mut reader = source.reader().await.unwrap();
        drop(source);
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
}

#[tokio::test]
async fn binary_empty_projection_keeps_a_row_and_storage_errors_propagate() {
    let request = ReadRequest::new("file").with_options(ReadOptions {
        projection: Some(vec![]),
        ..Default::default()
    });
    let source = open_attachment_source(
        MemoryAccessor::new(b"bytes", 2),
        request,
        AttachmentOptions::default(),
    )
    .await
    .unwrap();
    let batch = source
        .reader()
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.num_columns(), 0);
    assert_eq!(batch.num_rows(), 1);
    let source = open_attachment_source(
        MemoryAccessor::new(b"bytes", 2).with_failure(0, ErrorKind::Io),
        ReadRequest::new("file"),
        AttachmentOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        source
            .reader()
            .await
            .unwrap()
            .next()
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Io
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

#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
async fn labels(source: &BoundSource) -> Vec<String> {
    let mut reader = source.reader().await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.num_rows() <= 1);
        values.extend(
            batch["label"]
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_owned()),
        );
    }
    values
}

#[tokio::test]
#[cfg(any(
    feature = "format-csv",
    feature = "format-json",
    feature = "format-xml"
))]
async fn structured_formats_preserve_projection_options_and_independent_readers() {
    let cases: Vec<(AttachmentFormat, &[u8])> = vec![
        #[cfg(feature = "format-csv")]
        (AttachmentFormat::Csv, b"id,label\n1,a\n2,b\n"),
        #[cfg(feature = "format-csv")]
        (AttachmentFormat::Tsv, b"id\tlabel\n1\ta\n2\tb\n"),
        #[cfg(feature = "format-json")]
        (
            AttachmentFormat::Json,
            b"{\"id\":1,\"label\":\"a\"} {\"id\":2,\"label\":\"b\"}",
        ),
        #[cfg(feature = "format-json")]
        (
            AttachmentFormat::Jsonl,
            b"{\"id\":1,\"label\":\"a\"}\n{\"id\":2,\"label\":\"b\"}\n",
        ),
        #[cfg(feature = "format-xml")]
        (
            AttachmentFormat::Xml {
                record_path: "/rows/row".into(),
            },
            b"<rows><row><label>a</label></row><row><label>b</label></row></rows>",
        ),
    ];
    for (format, plain) in cases {
        for compressed in [false, true] {
            if compressed && !cfg!(feature = "decompression-gzip") {
                continue;
            }
            let bytes = if compressed {
                gzip(plain)
            } else {
                plain.to_vec()
            };
            let input = MemoryAccessor::new(&bytes, 3);
            let observations = input.observations.clone();
            let request = ReadRequest::new(if compressed { "opaque.gz" } else { "opaque" })
                .with_options(ReadOptions {
                    batch_size: NonZeroUsize::MIN,
                    projection: Some(vec!["label".into()]),
                })
                .with_byte_options(ByteReadOptions {
                    io_chunk_size: NonZeroUsize::new(2),
                    ..Default::default()
                });
            let source =
                open_attachment_source(input, request, AttachmentOptions::new(format.clone()))
                    .await
                    .unwrap();
            let opens = observations.requests.lock().unwrap().len();
            assert_eq!(labels(&source).await, ["a", "b"]);
            assert_eq!(
                observations.requests.lock().unwrap().len(),
                opens,
                "binding retains the first reader"
            );
            assert_eq!(labels(&source).await, ["a", "b"]);
            assert!(observations.requests.lock().unwrap().len() > opens);
            assert!(
                observations
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| r.options.io_chunk_size == NonZeroUsize::new(2))
            );
        }
    }
}

#[tokio::test]
#[cfg(all(feature = "format-json", not(feature = "decompression-gzip")))]
async fn disabled_outer_codec_fails_before_byte_io() {
    let input = MemoryAccessor::new(&gzip(b"{\"label\":\"a\"}"), 2);
    let observations = input.observations.clone();
    let error = open_attachment_source(
        input,
        ReadRequest::new("file.gz"),
        AttachmentOptions::new(AttachmentFormat::Json),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(observations.requests.lock().unwrap().is_empty());
}

#[tokio::test]
#[cfg(feature = "format-json")]
async fn json_aliases_keep_the_existing_object_stream_contract() {
    for format in [AttachmentFormat::Json, AttachmentFormat::Jsonl] {
        let error = open_attachment_source(
            MemoryAccessor::new(b"[{\"id\":1}]", 4),
            ReadRequest::new("file.json"),
            AttachmentOptions::new(format),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind(), ErrorKind::Decode);
    }
}

#[tokio::test]
#[cfg(feature = "format-parquet")]
async fn parquet_keeps_native_ranges_and_embedded_schema() {
    let bytes = open_arrow_access_test_support::fixture!("parquet", "ids.parquet").await;
    let input = MemoryAccessor::new(&bytes, 8).with_ranges();
    let observations = input.observations.clone();
    let source = open_attachment_source(
        input,
        ReadRequest::new("file.parquet"),
        AttachmentOptions::new(AttachmentFormat::Parquet),
    )
    .await
    .unwrap();
    let mut reader = source.reader().await.unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend(
            batch["id"]
                .as_primitive::<arrow_array::types::Int64Type>()
                .values(),
        );
    }
    assert_eq!(ids, [1, 2, 3]);
    assert!(
        observations
            .range_reads
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
}
