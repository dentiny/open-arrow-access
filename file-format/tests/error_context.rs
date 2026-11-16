#![cfg(any(feature = "warc", feature = "json", feature = "atif"))]

use open_arrow_access_core::{
    Accessor, Error, ErrorKind, ReadRequest, Reader, RecordReader, ScanPlanner,
};
use open_arrow_access_test_support::MemoryAccessor;

fn assert_object(error: &Error, locator: &str) {
    assert_eq!(error.kind(), ErrorKind::Decode, "{error}");
    assert_eq!(error.object().unwrap().locator(), locator);
    assert!(error.to_string().contains(locator), "{error}");
}

async fn read_error(mut reader: Box<dyn RecordReader>) -> Error {
    loop {
        match reader.next().await {
            Err(error) => {
                assert!(reader.next().await.unwrap().is_none());
                return error;
            }
            Ok(Some(_)) => (),
            Ok(None) => panic!("expected decode failure"),
        }
    }
}

#[cfg(feature = "warc")]
#[tokio::test]
async fn truncated_warc_gzip_names_object_in_sequential_and_planned_reads() {
    use open_arrow_access_core::AccessorExt;
    use open_arrow_access_decompression::AutoDecompressionLayer;
    use open_arrow_access_file_format::warc::{WarcAccessor, WarcArrowAccessor};
    use std::io::Write;

    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(b"WARC/1.0\r\n").unwrap();
    let mut bytes = gzip.finish().unwrap();
    bytes.truncate(bytes.len() - 4);
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
        let mut planned = files
            .plan_scan(ReadRequest::new(locator))
            .await
            .unwrap()
            .into_reader();
        let error = loop {
            match planned.next().await {
                Err(error) => break error,
                Ok(Some(_)) => (),
                Ok(None) => panic!("expected decode failure"),
            }
        };
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
