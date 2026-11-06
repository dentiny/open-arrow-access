use std::{error::Error as _, num::NonZeroUsize};

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, ByteReadRequest, Bytes, ErrorKind, ReadOptions,
    ReadRequest,
};

use super::super::{HuggingFaceOptions, HuggingFaceRepository, HuggingFaceService};
use super::support::{MockHub, TOKEN, assert_ranges};

#[tokio::test]
async fn constructor_is_lazy_send_and_redacts_token() {
    let options = HuggingFaceOptions::default()
        .with_endpoint("https://huggingface.co")
        .with_token(TOKEN)
        .with_http_downloads();
    assert!(!format!("{options:?}").contains(TOKEN));
    let service = tokio::spawn(HuggingFaceService::new(options))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(service.options.endpoint(), "https://huggingface.co");
    let accessor = service
        .repository(HuggingFaceRepository::new("owner/example"))
        .await
        .unwrap();
    assert!(!format!("{accessor:?}").contains(TOKEN));
    assert!(!format!("{service:?}").contains(TOKEN));
    assert!(accessor.capability().read_with_range);
    assert!(accessor.capability().list);
    let error = service
        .repository(HuggingFaceRepository::new(""))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.source().is_none());
}

#[tokio::test]
async fn ranges_preserve_order_options_metadata_and_sequential_cursor() {
    let contents = Bytes::from_static(b"abcdefghijklmnop");
    let hub = MockHub::file("bytes.bin", contents.clone());
    let accessor = hub
        .accessor()
        .with_chunk_size(NonZeroUsize::new(3).unwrap());
    let mut reader = accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_concurrency: NonZeroUsize::new(2).unwrap(),
                io_chunk_size: NonZeroUsize::new(2),
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.metadata().content_length, Some(16));
    assert_eq!(
        reader.metadata().source.as_ref().unwrap().locator(),
        "bytes.bin"
    );
    assert!(!reader.capability().list);
    let ranges = reader
        .read_ranges(vec![8..10, 0..2, 8..10, 4..4])
        .await
        .unwrap();
    assert_eq!(
        ranges.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
        [b"ij".as_ref(), b"ab", b"ij", b""]
    );
    let requests = hub.captured();
    assert_ranges(&requests, &["bytes=0-1", "bytes=8-9"]);
    assert!(requests.iter().all(|r| !r.uri.contains(TOKEN)));
    assert!(
        requests
            .iter()
            .any(|r| r.uri.contains("/paths-info/refs%2Fconvert%2Fparquet")
                && r.form.contains("paths=data/bytes.bin"))
    );
    let before_split = hub.request_count();
    assert_eq!(reader.read_range(0..8).await.unwrap().as_ref(), b"abcdefgh");
    assert_ranges(
        &hub.captured()[before_split..],
        &["bytes=0-1", "bytes=2-3", "bytes=4-5", "bytes=6-7"],
    );
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"abc");
    let before = hub.request_count();
    assert_eq!(reader.read_range(16..16).await.unwrap().len(), 0);
    for range in [16..17, std::ops::Range { start: 2, end: 1 }] {
        let error = reader.read_range(range).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "read_range");
        assert!(error.source().is_none());
    }
    let error = reader.read_ranges(vec![0..2, 16..17]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.source().is_none());
    assert_eq!(hub.request_count(), before);
    let chunks = open_arrow_access_core::into_stream(reader)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(chunks.concat(), contents[3..]);
    assert!(chunks.iter().all(|c| c.len() <= 3));
}

#[tokio::test]
async fn sequential_output_chunks_reuse_io_requests_and_leave_ranges_independent() {
    let bytes = Bytes::from_static(b"abcdefghijklmnop");
    let hub = MockHub::file("bytes.bin", bytes.clone());
    let accessor = hub.accessor().with_chunk_size(NonZeroUsize::MIN);
    let mut reader = accessor
        .open(
            ByteReadRequest::new("bytes.bin").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(4),
                ..ByteReadOptions::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"a");
    assert_eq!(reader.read_range(12..14).await.unwrap().as_ref(), b"mn");
    let mut output = b"a".to_vec();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.as_ptr(), bytes[output.len()..].as_ptr());
        output.extend_from_slice(&chunk);
    }
    assert_eq!(output, bytes);
    let requests = hub.captured();
    assert_ranges(
        &requests,
        &[
            "bytes=0-3",
            "bytes=4-7",
            "bytes=8-11",
            "bytes=12-15",
            "bytes=12-13",
        ],
    );
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(hub.request_count(), requests.len());
}

#[tokio::test]
async fn listing_recurses_and_follows_hub_pagination_without_exposing_directories() {
    let hub = MockHub::with_files([
        ("data/a.bin".to_owned(), Bytes::from_static(b"a")),
        ("data/nested/b.bin".to_owned(), Bytes::from_static(b"b")),
    ]);
    let accessor = hub.accessor();
    let objects = accessor
        .list("".into())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        objects.iter().map(|o| o.locator()).collect::<Vec<_>>(),
        ["a.bin", "nested/b.bin"]
    );
    let requests = hub.captured();
    assert!(requests.iter().any(|r| r.uri.contains("recursive=True")));
    assert!(requests.iter().any(|r| r.uri.contains("cursor=second")));
    let error = accessor.list("a.bin".into()).await.err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn invalid_paths_fail_before_requests_and_empty_files_are_readable() {
    let hub = MockHub::file("empty.bin", Bytes::new());
    let accessor = hub.accessor();
    for path in [
        "",
        " bytes.bin",
        "bytes.bin ",
        "/bytes.bin",
        "./bytes.bin",
        "../bytes.bin",
        "a//b",
        "a/../b",
        "a\\b",
        "a\0b",
        "a/",
    ] {
        let error = accessor.open(path.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{path:?}");
        assert_eq!(error.operation(), "open");
        assert!(error.source().is_none());
    }
    for path in ["/", "/a", "a//", "./", "../", " a/"] {
        let error = accessor.list(path.into()).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{path:?}");
        assert_eq!(error.operation(), "list");
        assert!(error.source().is_none());
    }
    assert!(hub.captured().is_empty());
    let mut reader = accessor.open("empty.bin".into()).await.unwrap();
    assert_eq!(reader.metadata().content_length, Some(0));
    assert!(reader.next().await.unwrap().is_none());
    assert!(reader.read_range(0..0).await.unwrap().is_empty());
    assert_eq!(hub.request_count(), 1);
}

#[tokio::test]
async fn parquet_reads_through_the_hub_sdk_and_shared_range_adapter() {
    use arrow_array::Int64Array;
    use open_arrow_access_file_format::ParquetAccessor;

    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata/parquet/ids.parquet");
    let bytes = Bytes::from(std::fs::read(path).unwrap());
    let hub = MockHub::file("part.parquet", bytes);
    let accessor = ParquetAccessor::new(hub.accessor());
    let mut reader = accessor
        .open(ReadRequest::new("part.parquet").with_options(ReadOptions {
            batch_size: NonZeroUsize::new(2).unwrap(),
            projection: None,
        }))
        .await
        .unwrap();
    let mut values = Vec::<i64>::new();
    while let Some(batch) = reader.next().await.unwrap() {
        values.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(values, [1, 2, 3]);
    assert!(hub.captured().iter().any(|r| r.range.is_some()));
}
