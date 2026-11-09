use http::{Response, StatusCode};
use open_arrow_access_service::common::Connection;
use open_arrow_access_test_support::http::HttpFixture;
use opendal::{Buffer, HttpBody};
mod providers;
use providers::connection;
#[tokio::test]
async fn reads_one_page_and_strips_credentials_across_redirects() {
    let f = HttpFixture::new();
    let dest = HttpFixture::new();
    let location = format!("{}/page?sig=a%2Bb", dest.endpoint());
    f.handle(move |r| {
        assert_eq!(r.headers()["x-api-key"], "secret");
        assert_eq!(r.uri().query(), Some("api_key=secret"));
        Ok(Response::builder()
            .status(302)
            .header("location", &location)
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    dest.respond(
        "/page?sig=a%2Bb",
        StatusCode::OK,
        Buffer::from(" { \"records\": [] } ").to_bytes(),
    );
    let c = connection(&f)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "secret");
    let doc = c.json("start", &[]).await.unwrap();
    assert!(doc.value()["records"].is_array());
    assert!(!dest.requests()[0].headers.contains_key("x-api-key"));
    assert!(!format!("{c:?}").contains("secret"));
    assert_eq!(f.requests().len(), 1);
    assert_eq!(dest.requests().len(), 1);
}
#[tokio::test]
async fn scoped_query_survives_redirect_and_cannot_duplicate_existing_key() {
    let f = HttpFixture::new();
    f.handle(|r| {
        assert_eq!(r.uri().query(), Some("api_key=secret"));
        let response = if r.uri().path() == "/start" {
            Response::builder()
                .status(302)
                .header("location", "/finish")
        } else {
            Response::builder().status(200)
        };
        Ok(response
            .body(HttpBody::new(
                futures::stream::iter([Ok(Buffer::from("{}"))]),
                Some(2),
            ))
            .unwrap())
    });
    connection(&f)
        .with_query("api_key", "secret")
        .json("start", &[("api_key", "secret")])
        .await
        .unwrap();
    assert_eq!(f.requests().len(), 2);
}
#[tokio::test]
async fn validates_endpoint_header_and_api_continuation() {
    assert!(Connection::new("https://u:p@example.com").is_err());
    let f = HttpFixture::new();
    let c = connection(&f);
    assert!(c.with_header("bad\nheader", "value").is_err());
    assert!(
        connection(&f)
            .json_url(url::Url::parse("https://elsewhere.invalid").unwrap())
            .await
            .is_err()
    );
    assert!(f.requests().is_empty());
}

#[tokio::test]
async fn redirect_limits_and_https_downgrades_are_decode_errors() {
    use open_arrow_access_core::ErrorKind;
    let f = HttpFixture::new();
    f.handle(|_| {
        Ok(Response::builder()
            .status(302)
            .header("location", "/loop")
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    assert_eq!(
        connection(&f).json("loop", &[]).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert_eq!(f.requests().len(), 11);

    let f = HttpFixture::new();
    let location = format!("{}/plain", f.endpoint().replacen("https://", "http://", 1));
    f.handle(move |_| {
        Ok(Response::builder()
            .status(302)
            .header("location", &location)
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    assert_eq!(
        connection(&f).json("secure", &[]).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert_eq!(f.requests().len(), 1);
}

#[tokio::test]
async fn buffered_and_streaming_reads_validate_declared_body_length() {
    use open_arrow_access_core::{ByteReadOptions, ErrorKind};

    for (declared, payload) in [(1, "{}"), (3, "{}"), (1, "")] {
        let f = HttpFixture::new();
        f.handle(move |_| {
            Ok(Response::builder()
                .status(200)
                .header("content-length", declared)
                // The body itself has no length hint: validation belongs to the client.
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from(payload))]),
                    None,
                ))
                .unwrap())
        });
        let c = connection(&f);
        let error = c.json("metadata", &[]).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "provider_http");

        let mut reader = c
            .open(
                &format!("{}/file", f.endpoint()),
                ByteReadOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(reader.metadata().content_length, Some(declared));
        let error = loop {
            match reader.next().await {
                Err(error) => break error,
                Ok(Some(_)) => {}
                Ok(None) => panic!("accepted a body with a mismatched length"),
            }
        };
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "provider_http");
        let requests = f.requests().len();
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(f.requests().len(), requests);
    }
}

#[tokio::test]
async fn body_errors_preserve_category_and_cause_in_both_read_paths() {
    use open_arrow_access_core::{ByteReadOptions, Error, ErrorKind};
    use std::error::Error as _;

    let f = HttpFixture::new();
    f.handle(|_| {
        let source =
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "injected body fault");
        let shared =
            Error::new(ErrorKind::Timeout, "fixture_body", "body failure").with_source(source);
        let error =
            opendal::Error::new(opendal::ErrorKind::Unexpected, "body failure").set_source(shared);
        Ok(Response::builder()
            .status(200)
            .body(HttpBody::new(futures::stream::iter([Err(error)]), None))
            .unwrap())
    });
    let c = connection(&f);
    let buffered = c.json("metadata", &[]).await.unwrap_err();
    let mut reader = c
        .open(
            &format!("{}/file", f.endpoint()),
            ByteReadOptions::default(),
        )
        .await
        .unwrap();
    let streaming = reader.next().await.unwrap_err();
    for error in [buffered, streaming] {
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.operation(), "provider_http");
        let mut source = error.source();
        let mut original = None;
        while let Some(cause) = source {
            if let Some(cause) = cause.downcast_ref::<std::io::Error>() {
                original = Some(cause.kind());
            }
            source = cause.source();
        }
        assert_eq!(original, Some(std::io::ErrorKind::ConnectionReset));
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn absolute_signed_downloads_do_not_receive_provider_credentials() {
    use open_arrow_access_core::{ByteReadOptions, read_all};

    let provider = HttpFixture::new();
    let download = HttpFixture::new();
    download.file("/path%20one?sig=a%2Bb%2Fz", "payload");
    let c = connection(&provider)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "secret");
    let url = format!("{}/path%20one?sig=a%2Bb%2Fz", download.endpoint());
    let reader = c.open(&url, ByteReadOptions::default()).await.unwrap();
    assert_eq!(read_all(reader, "test").await.unwrap(), b"payload");
    assert!(provider.requests().is_empty());
    let requests = download.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/path%20one?sig=a%2Bb%2Fz");
    assert!(!requests[0].headers.contains_key("x-api-key"));
}

#[tokio::test]
async fn credentials_stay_stripped_after_redirect_returns_to_provider() {
    let provider = HttpFixture::new();
    let external = HttpFixture::new();
    let external_url = format!("{}/bounce", external.endpoint());
    provider.handle(move |request| {
        if request.uri().path() == "/start" {
            assert_eq!(request.headers()["x-api-key"], "secret");
            assert_eq!(request.uri().query(), Some("api_key=secret"));
            Ok(Response::builder()
                .status(302)
                .header("location", &external_url)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        } else {
            assert_eq!(request.uri().path(), "/finish");
            assert!(!request.headers().contains_key("x-api-key"));
            assert!(request.uri().query().is_none());
            Ok(Response::builder()
                .status(200)
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from("{}"))]),
                    Some(2),
                ))
                .unwrap())
        }
    });
    let return_url = format!("{}/finish", provider.endpoint());
    external.handle(move |request| {
        assert!(!request.headers().contains_key("x-api-key"));
        assert!(request.uri().query().is_none());
        Ok(Response::builder()
            .status(302)
            .header("location", &return_url)
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    connection(&provider)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "secret")
        .json("start", &[])
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(external.requests().len(), 1);
}
