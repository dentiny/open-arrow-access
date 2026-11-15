use std::{num::NonZeroUsize, ops::Range};

use futures::stream;
use http::{Request, Response, StatusCode};
use open_arrow_access_core::{Accessor, ByteReadOptions, Bytes, ErrorKind, ReadRequest};
use open_arrow_access_service::common::MetadataLink;
use open_arrow_access_test_support::{collect_bytes, http::HttpFixture};
use opendal::{Buffer, HttpBody};

mod providers;

fn response(status: StatusCode, bytes: Bytes) -> Response<HttpBody> {
    Response::builder()
        .status(status)
        .header("content-length", bytes.len())
        .header("accept-ranges", "bytes")
        .header("etag", "\"v1\"")
        .body(HttpBody::new(stream::iter([Ok(Buffer::from(bytes))]), None))
        .unwrap()
}

fn serve(request: &Request<Buffer>, bytes: Bytes) -> Response<HttpBody> {
    assert_eq!(request.headers()["accept-encoding"], "identity");
    if let Some(range) = request.headers().get("range") {
        let (start, end) = range
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let range = start.parse::<usize>().unwrap()..end.parse::<usize>().unwrap() + 1;
        assert_eq!(request.headers()["if-match"], "\"v1\"");
        let mut output = response(StatusCode::PARTIAL_CONTENT, bytes.slice(range.clone()));
        output.headers_mut().insert(
            "content-range",
            format!("bytes {}-{}/{}", range.start, range.end - 1, bytes.len())
                .parse()
                .unwrap(),
        );
        output
    } else {
        response(StatusCode::OK, bytes)
    }
}

#[tokio::test]
async fn exact_ranges_preserve_cursor_aliases_signed_urls_and_io_options() {
    let f = HttpFixture::new();
    f.handle(|request| Ok(serve(&request, Bytes::from_static(b"abcdefgh"))));
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
    assert_eq!(
        reader
            .read_ranges(vec![5..8, 0..2, 8..8, 5..8])
            .await
            .unwrap(),
        [
            Bytes::from_static(b"fgh"),
            Bytes::from_static(b"ab"),
            Bytes::new(),
            Bytes::from_static(b"fgh")
        ]
    );
    assert_eq!(reader.read_range(0..0).await.unwrap(), Bytes::new());
    let count = f.requests().len();
    for range in [Range { start: 3, end: 2 }, 0..9, 9..9] {
        assert_eq!(
            reader.read_range(range).await.unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(f.requests().len(), count);
    assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"cdefgh");
    assert_eq!(reader.read_range(2..4).await.unwrap().as_ref(), b"cd");
    assert!(reader.next().await.unwrap().is_none());
    assert!(f.requests().iter().all(|r| r.path == "/file?sig=a%2Bb%2Fz"));
}

#[tokio::test]
async fn unsupported_or_unknown_representations_remain_sequential() {
    for case in ["absent", "none", "unknown-length", "encoded"] {
        let f = HttpFixture::new();
        f.handle(move |_| {
            let mut output = response(StatusCode::OK, Bytes::from_static(b"abc"));
            match case {
                "absent" => {
                    output.headers_mut().remove("accept-ranges");
                }
                "none" => {
                    output
                        .headers_mut()
                        .insert("accept-ranges", "none".parse().unwrap());
                }
                "unknown-length" => {
                    output.headers_mut().remove("content-length");
                }
                _ => {
                    output
                        .headers_mut()
                        .insert("content-encoding", "gzip".parse().unwrap());
                }
            }
            Ok(output)
        });
        let mut reader = providers::connection(&f)
            .open(&format!("{}/file", f.endpoint()), Default::default())
            .await
            .unwrap();
        assert!(!reader.capability().read_with_range, "{case}");
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
    for case in [
        "ignored",
        "missing",
        "bounds",
        "total",
        "wildcard",
        "duplicate",
        "short",
        "long",
        "length",
        "encoding",
        "etag",
        "missing-etag",
        "precondition",
        "unsatisfiable",
    ] {
        let f = HttpFixture::new();
        f.handle(move |request| {
            let mut output = serve(&request, Bytes::from_static(b"abcdefgh"));
            if request.headers().contains_key("range") {
                match case {
                    "ignored" => *output.status_mut() = StatusCode::OK,
                    "missing" => {
                        output.headers_mut().remove("content-range");
                    }
                    "bounds" => {
                        output
                            .headers_mut()
                            .insert("content-range", "bytes 0-1/8".parse().unwrap());
                    }
                    "total" => {
                        output
                            .headers_mut()
                            .insert("content-range", "bytes 2-3/9".parse().unwrap());
                    }
                    "wildcard" => {
                        output
                            .headers_mut()
                            .insert("content-range", "bytes 2-3/*".parse().unwrap());
                    }
                    "duplicate" => {
                        output
                            .headers_mut()
                            .append("content-range", "bytes 2-3/8".parse().unwrap());
                    }
                    "short" | "long" => {
                        output.headers_mut().remove("content-length");
                        *output.body_mut() = HttpBody::new(
                            stream::iter([Ok(Buffer::from(if case == "short" {
                                "c"
                            } else {
                                "cde"
                            }))]),
                            None,
                        );
                    }
                    "length" => {
                        output
                            .headers_mut()
                            .insert("content-length", "3".parse().unwrap());
                    }
                    "encoding" => {
                        output
                            .headers_mut()
                            .insert("content-encoding", "gzip".parse().unwrap());
                    }
                    "etag" => {
                        output
                            .headers_mut()
                            .insert("etag", "\"v2\"".parse().unwrap());
                    }
                    "missing-etag" => {
                        output.headers_mut().remove("etag");
                    }
                    "precondition" => *output.status_mut() = StatusCode::PRECONDITION_FAILED,
                    _ => *output.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE,
                }
            }
            Ok(output)
        });
        let mut reader = providers::connection(&f)
            .open(&format!("{}/file", f.endpoint()), Default::default())
            .await
            .unwrap();
        let error = reader.read_range(2..4).await.unwrap_err();
        match case {
            "ignored" => assert_eq!(error.kind(), ErrorKind::Unsupported),
            "precondition" => assert_eq!(error.http_status(), Some(412)),
            "unsatisfiable" => assert_eq!(error.http_status(), Some(416)),
            _ => assert_eq!(error.kind(), ErrorKind::Decode, "{case}: {error}"),
        }
        assert_eq!(collect_bytes(reader.as_mut()).await.unwrap(), b"abcdefgh");
    }
}

#[tokio::test]
async fn remote_service_parquet_decodes_through_ranges() {
    use arrow_array::Int64Array;
    use open_arrow_access_file_format::parquet::ParquetAccessor;
    let bytes = Bytes::from(
        tokio::fs::read(open_arrow_access_test_support::fixture_path!(
            "parquet",
            "ordered-uncompressed.parquet"
        ))
        .await
        .unwrap(),
    );
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
    let mut reader = ParquetAccessor::new(files)
        .open(ReadRequest::new("data.parquet"))
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
    }
    assert_eq!(ids, (0..7).collect::<Vec<_>>());
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
    let return_url = format!("{}/finish?sig=c%2Bd", provider.endpoint());
    provider.handle(move |request| {
        if request.uri().path() != "/finish" {
            assert_eq!(request.headers()["x-api-key"], "secret");
            assert_eq!(request.uri().query(), Some("api_key=secret"));
            Ok(if request.uri().path() == "/start" {
                redirect("/same")
            } else {
                redirect(&external_url)
            })
        } else {
            assert!(!request.headers().contains_key("x-api-key"));
            assert_eq!(request.uri().query(), Some("sig=c%2Bd"));
            Ok(serve(&request, Bytes::from_static(b"abcdefgh")))
        }
    });
    external.handle(move |request| {
        assert!(!request.headers().contains_key("x-api-key"));
        assert_eq!(request.uri().query(), Some("sig=a%2Bb"));
        Ok(redirect(&return_url))
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
    assert_eq!(provider.requests().len(), 6);
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
            let ranged = request.headers().contains_key("range");
            let mut output = response(
                if ranged {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                },
                Bytes::from_static(if ranged { b"ab" } else { b"abcdefgh" }),
            );
            if ranged {
                output
                    .headers_mut()
                    .insert("content-range", "bytes 0-1/8".parse().unwrap());
            }
            if let Some(etag) = etag {
                output.headers_mut().insert("etag", etag.parse().unwrap());
            } else {
                output.headers_mut().remove("etag");
            }
            Ok(output)
        });
        let mut reader = providers::connection(&f)
            .open(&format!("{}/file", f.endpoint()), Default::default())
            .await
            .unwrap();
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
