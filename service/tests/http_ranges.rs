use std::{num::NonZeroUsize, ops::Range};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReader, Bytes, ErrorKind, ReadRequest,
};
use open_arrow_access_service::common::MetadataLink;
use open_arrow_access_test_support::{
    byte_accessor::assert_ranges,
    collect_bytes, fixture,
    http::{HttpFixture, file_response, response},
    record::collect_column,
};
use opendal::{Buffer, HttpBody};

mod providers;

async fn open(f: &HttpFixture) -> Box<dyn ByteReader> {
    providers::connection(f)
        .open(&format!("{}/file", f.endpoint()), Default::default())
        .await
        .unwrap()
}

fn set_header(output: &mut Response<HttpBody>, name: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        output.headers_mut().insert(name, value.parse().unwrap());
    } else {
        output.headers_mut().remove(name);
    }
}

fn serve(request: &Request<Buffer>, bytes: Bytes) -> Response<HttpBody> {
    assert_eq!(request.headers()["accept-encoding"], "identity");
    let mut output = file_response(request, bytes);
    set_header(&mut output, "accept-ranges", Some("bytes"));
    set_header(&mut output, "etag", Some("\"v1\""));
    output
}

#[tokio::test]
async fn exact_ranges_preserve_cursor_aliases_signed_urls_and_io_options() {
    let f = HttpFixture::new();
    f.handle(|request| {
        let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
        if request.headers().contains_key("range") {
            assert_eq!(request.headers()["if-match"], "\"v1\"");
        } else {
            // Include empty parts and parts larger than the requested chunk size.
            let parts = [b"".as_slice(), b"abcd", b"", b"efgh"].map(Bytes::from_static);
            *output.body_mut() =
                HttpBody::new(stream::iter([Ok(Buffer::from(parts.to_vec()))]), None);
        }
        Ok(output)
    });
    let files = providers::connection(&f)
        .files(
            vec![MetadataLink {
                file_name: "alias".into(),
                url: format!("{}/file?sig=a%2Bb%2Fz", f.endpoint()),
            }],
            (),
        )
        .unwrap();
    assert!(files.capability().read_with_range);
    let mut reader = files
        .open(
            open_arrow_access_core::ByteReadRequest::new("alias").with_options(ByteReadOptions {
                io_chunk_size: NonZeroUsize::new(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(reader.capability().read_with_range);
    assert_eq!(reader.metadata().version.as_deref(), Some("\"v1\""));
    assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), b"ab");
    assert_ranges(reader.as_mut(), b"abcdefgh", &[5..8, 0..2, 8..8, 5..8])
        .await
        .unwrap();
    let count = f.requests().len();
    for range in [Range { start: 3, end: 2 }, 0..9, 9..9] {
        assert_eq!(
            reader.read_range(range).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(f.requests().len(), count);
    for expected in [b"cd", b"ef", b"gh"] {
        assert_eq!(reader.next().await.unwrap().unwrap().as_ref(), expected);
    }
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reader.read_range(2..4).await.unwrap().as_ref(), b"cd");
    assert!(reader.next().await.unwrap().is_none());
    assert!(f.requests().iter().all(|r| r.path == "/file?sig=a%2Bb%2Fz"));
}

#[tokio::test]
async fn unsupported_or_unknown_representations_remain_sequential() {
    for (name, value) in [
        ("accept-ranges", None),
        ("accept-ranges", Some("none")),
        ("content-length", None),
        ("content-encoding", Some("gzip")),
    ] {
        let f = HttpFixture::new();
        f.handle(move |request| {
            let mut output = serve(&request, Bytes::from_static(b"abc"));
            set_header(&mut output, name, value);
            Ok(output)
        });
        let mut reader = open(&f).await;
        assert!(!reader.capability().read_with_range, "{name}: {value:?}");
        assert_eq!(
            reader.read_range(0..1).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abc");
        assert_eq!(f.requests().len(), 1);
    }
}

#[tokio::test]
async fn malformed_or_changed_partial_responses_are_rejected() {
    for (field, value) in [
        ("status", Some("200")),
        ("content-range", None),
        ("content-range", Some("bytes 0-1/8")),
        ("content-range", Some("bytes 2-3/9")),
        ("content-range", Some("bytes 2-3/*")),
        ("content-range", Some("bytes invalid/8")),
        ("content-range", Some("bytes 2-18446744073709551615/8")),
        ("content-range", Some("bytes 3-2/8")),
        ("duplicate-content-range", Some("bytes 2-3/8")),
        ("body", Some("c")),
        ("body", Some("cde")),
        ("content-length", Some("3")),
        ("content-encoding", Some("gzip")),
        ("etag", Some("\"v2\"")),
        ("etag", None),
        ("status", Some("412")),
        ("status", Some("416")),
    ] {
        let f = HttpFixture::new();
        f.handle(move |request| {
            let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
            if request.headers().contains_key("range") {
                match field {
                    "status" => *output.status_mut() = value.unwrap().parse().unwrap(),
                    "body" => {
                        output.headers_mut().remove("content-length");
                        *output.body_mut() =
                            HttpBody::new(stream::iter([Ok(Buffer::from(value.unwrap()))]), None);
                    }
                    "duplicate-content-range" => {
                        output
                            .headers_mut()
                            .append("content-range", value.unwrap().parse().unwrap());
                    }
                    name => set_header(&mut output, name, value),
                }
            }
            Ok(output)
        });
        let mut reader = open(&f).await;
        let error = reader.read_range(2..4).await.unwrap_err();
        match (field, value) {
            ("status", Some("200")) => assert_eq!(error.kind(), ErrorKind::Unsupported),
            ("status", Some(code)) => assert_eq!(error.http_status(), Some(code.parse().unwrap())),
            _ => assert_eq!(
                error.kind(),
                ErrorKind::Decode,
                "{field}: {value:?}: {error}"
            ),
        }
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdefgh");
    }
}

#[tokio::test]
async fn remote_service_parquet_decodes_through_ranges() {
    use arrow_array::types::Int64Type;
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let bytes = Bytes::from(fixture!("parquet", "ordered-uncompressed.parquet").await);
    let f = HttpFixture::new();
    f.handle(move |request| Ok(serve(&request, bytes.clone())));
    let files = providers::connection(&f)
        .files(
            vec![MetadataLink {
                file_name: "data.parquet".into(),
                url: format!("{}/download", f.endpoint()),
            }],
            (),
        )
        .unwrap();
    let reader = ParquetAccessor::new(files)
        .open(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    assert_eq!(
        collect_column::<Int64Type>(reader, 0).await,
        (0..7).collect::<Vec<_>>()
    );
    let requests = f.requests();
    assert!(requests.iter().any(|r| r.headers.contains_key("range")));
    assert!(requests.iter().all(|r| r.method == http::Method::GET));
}

fn redirect(location: &str) -> Response<HttpBody> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", location)
        .body(HttpBody::new(stream::empty(), Some(0)))
        .unwrap()
}

#[tokio::test]
async fn ranges_keep_protocol_headers_and_strip_credentials_after_leaving_origin() {
    let provider = HttpFixture::new();
    let external = HttpFixture::new();
    let external_url = format!("{}/bounce?sig=a%2Bb", external.endpoint());
    provider.handle(move |request| {
        assert_eq!(request.headers()["x-api-key"], "secret");
        assert_eq!(request.uri().query(), Some("api_key=secret"));
        Ok(redirect(&external_url))
    });
    external.handle(move |request| {
        assert!(!request.headers().contains_key("x-api-key"));
        assert_eq!(request.uri().query(), Some("sig=a%2Bb"));
        if request.headers().contains_key("range") {
            assert_eq!(request.headers()["if-match"], "\"v1\"");
        }
        Ok(serve(&request, Bytes::from_static(b"abcdefgh")))
    });
    let connection = providers::connection(&provider)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "secret")
        .with_header("range", "bytes=7-7")
        .unwrap()
        .with_header("accept-encoding", "gzip")
        .unwrap()
        .with_header("if-match", "\"wrong\"")
        .unwrap();
    let mut reader = connection
        .open(
            &format!("{}/start", provider.endpoint()),
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(reader.read_range(1..3).await.unwrap().as_ref(), b"bc");
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdefgh");
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(external.requests().len(), 2);
}

#[tokio::test]
async fn range_requests_share_retry_policy() {
    use open_arrow_access_storage::{RetryConfig, StorageConfig};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    let f = HttpFixture::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    f.handle(move |request| {
        if request.headers().contains_key("range") && observed.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(response(StatusCode::TOO_MANY_REQUESTS, Bytes::new()))
        } else {
            Ok(serve(&request, Bytes::from_static(b"abcdefgh")))
        }
    });
    let connection = providers::connection(&f)
        .with_http_config(StorageConfig {
            retry: Some(RetryConfig {
                max_retries: 1,
                min_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                jitter: false,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
    let mut reader = connection
        .open(&format!("{}/file", f.endpoint()), Default::default())
        .await
        .unwrap();
    assert_eq!(reader.read_range(0..2).await.unwrap().as_ref(), b"ab");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn weak_and_absent_etags_do_not_send_strong_preconditions() {
    for etag in [None, Some("W/\"weak\""), Some("\"one\", \"two\"")] {
        let f = HttpFixture::new();
        f.handle(move |request| {
            assert!(!request.headers().contains_key("if-match"));
            let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
            set_header(&mut output, "etag", etag);
            Ok(output)
        });
        let mut reader = open(&f).await;
        assert_eq!(reader.metadata().version.as_deref(), etag);
        assert_eq!(reader.read_range(0..2).await.unwrap().as_ref(), b"ab");
    }
}

#[tokio::test]
async fn range_body_timeout_is_not_replayed() {
    use open_arrow_access_storage::{StorageConfig, TimeoutConfig};
    use std::time::Duration;
    let f = HttpFixture::new();
    f.handle(|request| {
        let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
        if request.headers().contains_key("range") {
            *output.body_mut() = HttpBody::new(stream::pending(), None);
        }
        Ok(output)
    });
    let connection = providers::connection(&f)
        .with_http_config(StorageConfig {
            timeout: Some(TimeoutConfig {
                operation_timeout: Duration::from_secs(5),
                io_timeout: Duration::from_millis(5),
            }),
            ..Default::default()
        })
        .unwrap();
    let mut reader = connection
        .open(&format!("{}/file", f.endpoint()), Default::default())
        .await
        .unwrap();
    assert_eq!(
        reader.read_range(0..2).await.unwrap_err().kind(),
        ErrorKind::Timeout
    );
    assert_eq!(f.requests().len(), 2);
}
