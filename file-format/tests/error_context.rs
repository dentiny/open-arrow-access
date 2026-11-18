#![cfg(any(feature = "warc", feature = "json", feature = "atif"))]

use futures::TryStreamExt;
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, ReadRequest, RecordReader, ScanPlanner, into_stream,
};
use open_arrow_access_test_support::MemoryAccessor;
#[cfg(feature = "warc")]
use open_arrow_access_test_support::fixture;

fn assert_object(error: &Error, locator: &str) {
    assert_eq!(error.kind(), ErrorKind::Decode, "{error}");
    assert!(error.to_string().contains(locator), "{error}");
}

async fn read_error(reader: Box<dyn RecordReader>) -> Error {
    into_stream(reader)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
}

#[cfg(feature = "warc")]
#[tokio::test]
async fn truncated_warc_gzip_names_object_in_sequential_and_planned_reads() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_decompression::AutoDecompressionLayer;
    use open_arrow_access_file_format::warc::{WarcAccessor, WarcArrowAccessor};
    let bytes = fixture!("warc", "bad-truncated-header.warc.gz").await;
    for locator in [
        "b.warc.gz",
        "/tmp/b.warc.gz",
        "s3://bucket/prefix/b.warc.gz",
    ] {
        let input = MemoryAccessor::new(&bytes, 1).layer(AutoDecompressionLayer::new());
        let mut native = WarcAccessor::new(input.clone())
            .open(locator.into())
            .await
            .unwrap();
        let error = native.next().await.unwrap_err();
        assert_object(&error, locator);
        assert!(native.next().await.unwrap().is_none());
        let files = WarcArrowAccessor::new(input);
        assert_object(
            &read_error(files.open(ReadRequest::new(locator)).await.unwrap()).await,
            locator,
        );
        let planned = files
            .plan_scan(ReadRequest::new(locator))
            .await
            .unwrap()
            .into_reader();
        let error = read_error(Box::new(planned)).await;
        assert_object(&error, locator);
    }
}

#[cfg(feature = "json")]
#[tokio::test]
async fn inference_errors_name_object_during_open_and_planning() {
    use open_arrow_access_file_format::json::JsonAccessor;
    let files = JsonAccessor::infer(MemoryAccessor::new(b"{\"id\":", 1));
    let locator = "/data/bad.jsonl";
    let error = files.open(ReadRequest::new(locator)).await.err().unwrap();
    assert_object(&error, locator);
    let error = files
        .plan_scan(ReadRequest::new(locator))
        .await
        .err()
        .unwrap();
    assert_object(&error, locator);
}

#[cfg(feature = "atif")]
#[tokio::test]
async fn atif_keeps_exact_absolute_and_relative_locators() {
    use open_arrow_access_file_format::atif::{AtifAccessor, AtifFormat};
    for locator in ["data/bad.atif", "/data/bad.atif"] {
        let files = AtifAccessor::new(
            MemoryAccessor::new(b"{\"bad\":true}\n", 1),
            AtifFormat::JsonLines,
        );
        assert_object(
            &read_error(files.open(ReadRequest::new(locator)).await.unwrap()).await,
            locator,
        );
        assert_object(
            &read_error(Box::new(
                files
                    .plan_scan(ReadRequest::new(locator))
                    .await
                    .unwrap()
                    .into_reader(),
            ))
            .await,
            locator,
        );
    }
}
