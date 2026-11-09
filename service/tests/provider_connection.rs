use std::{error::Error as _, io, iter};

use futures::stream;
use http::{Response, StatusCode};
use open_arrow_access_core::{Error, ErrorKind, read_all};
use open_arrow_access_service::common::Connection;
use open_arrow_access_test_support::{
    collect_bytes,
    http::{HttpFixture, response},
};
use opendal::{Buffer, HttpBody};

mod providers;
use providers::connection;

fn redirect(location: &str) -> Response<HttpBody> {
    Response::builder()
        .status(StatusCode::FOUND)
        .header("location", location)
        .body(HttpBody::new(stream::empty(), Some(0)))
        .unwrap()
}

fn authenticated_connection(fixture: &HttpFixture) -> Connection {
    connection(fixture)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "secret")
}

#[tokio::test]
async fn reads_one_page_and_strips_credentials_across_redirects() {
    let provider = HttpFixture::new();
    let dest = HttpFixture::new();
    let location = format!("{}/page?sig=a%2Bb", dest.endpoint());
    provider.handle(move |request| {
        assert_eq!(request.headers()["x-api-key"], "secret");
        assert_eq!(request.uri().query(), Some("api_key=secret"));
        Ok(redirect(&location))
    });
    dest.respond("/page?sig=a%2Bb", StatusCode::OK, " { \"records\": [] } ");
    let client = authenticated_connection(&provider);
    assert!(client.json("start", &[]).await.unwrap().value()["records"].is_array());
    assert!(!dest.requests()[0].headers.contains_key("x-api-key"));
    assert!(!format!("{client:?}").contains("secret"));
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(dest.requests().len(), 1);
}

#[tokio::test]
async fn scoped_query_survives_redirect_and_cannot_duplicate_existing_key() {
    let fixture = HttpFixture::new();
    fixture.handle(|request| {
        assert_eq!(request.uri().query(), Some("api_key=secret"));
        Ok(if request.uri().path() == "/start" {
            redirect("/finish")
        } else {
            response(StatusCode::OK, "{}")
        })
    });
    connection(&fixture)
        .with_query("api_key", "secret")
        .json("start", &[("api_key", "secret")])
        .await
        .unwrap();
    assert_eq!(fixture.requests().len(), 2);
}

#[tokio::test]
async fn validates_endpoint_header_and_api_continuation() {
    assert!(Connection::new("https://u:p@example.com").is_err());
    let fixture = HttpFixture::new();
    assert!(
        connection(&fixture)
            .with_header("bad\nheader", "value")
            .is_err()
    );
    assert!(
        connection(&fixture)
            .json_url(url::Url::parse("https://elsewhere.invalid").unwrap())
            .await
            .is_err()
    );
    assert!(fixture.requests().is_empty());
}

#[tokio::test]
async fn redirect_limits_and_https_downgrades_are_decode_errors() {
    let fixture = HttpFixture::new();
    fixture.handle(|_| Ok(redirect("/loop")));
    assert_eq!(
        connection(&fixture)
            .json("loop", &[])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
    assert_eq!(fixture.requests().len(), 11);

    let fixture = HttpFixture::new();
    let location = format!(
        "{}/plain",
        fixture.endpoint().replacen("https://", "http://", 1)
    );
    fixture.handle(move |_| Ok(redirect(&location)));
    assert_eq!(
        connection(&fixture)
            .json("secure", &[])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Decode
    );
    assert_eq!(fixture.requests().len(), 1);
}

#[tokio::test]
async fn buffered_and_streaming_reads_validate_declared_body_length() {
    for (declared, payload) in [(1, "{}"), (3, "{}"), (1, "")] {
        let fixture = HttpFixture::new();
        fixture.handle(move |_| {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", declared)
                // Omit the body's length hint to exercise client validation.
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(payload))]),
                    None,
                ))
                .unwrap())
        });
        let client = connection(&fixture);
        let buffered = client.json("metadata", &[]).await.unwrap_err();
        let mut reader = client
            .open(&format!("{}/file", fixture.endpoint()), Default::default())
            .await
            .unwrap();
        assert_eq!(reader.metadata().content_length, Some(declared));
        let streaming = collect_bytes(reader.as_mut()).await.unwrap_err();
        for error in [buffered, streaming] {
            assert_eq!(error.kind(), ErrorKind::Decode);
            assert_eq!(error.operation(), "provider_http");
        }
        let requests = fixture.requests().len();
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(fixture.requests().len(), requests);
    }
}

#[tokio::test]
async fn body_errors_preserve_category_and_cause_in_both_read_paths() {
    let fixture = HttpFixture::new();
    fixture.handle(|_| {
        let source = io::Error::new(io::ErrorKind::ConnectionReset, "injected body fault");
        let shared =
            Error::new(ErrorKind::Timeout, "fixture_body", "body failure").with_source(source);
        let error =
            opendal::Error::new(opendal::ErrorKind::Unexpected, "body failure").set_source(shared);
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(HttpBody::new(stream::iter([Err(error)]), None))
            .unwrap())
    });
    let client = connection(&fixture);
    let buffered = client.json("metadata", &[]).await.unwrap_err();
    let mut reader = client
        .open(&format!("{}/file", fixture.endpoint()), Default::default())
        .await
        .unwrap();
    let streaming = reader.next().await.unwrap_err();
    for error in [buffered, streaming] {
        assert_eq!(error.kind(), ErrorKind::Timeout);
        assert_eq!(error.operation(), "provider_http");
        assert!(
            iter::successors(error.source(), |&cause| cause.source()).any(|cause| {
                cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|cause| cause.kind() == io::ErrorKind::ConnectionReset)
            })
        );
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn absolute_signed_downloads_do_not_receive_provider_credentials() {
    let provider = HttpFixture::new();
    let download = HttpFixture::new();
    download.file("/path%20one?sig=a%2Bb%2Fz", "payload");
    let client = authenticated_connection(&provider);
    let url = format!("{}/path%20one?sig=a%2Bb%2Fz", download.endpoint());
    let reader = client.open(&url, Default::default()).await.unwrap();
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
        Ok(if request.uri().path() == "/start" {
            assert_eq!(request.headers()["x-api-key"], "secret");
            assert_eq!(request.uri().query(), Some("api_key=secret"));
            redirect(&external_url)
        } else {
            assert_eq!(request.uri().path(), "/finish");
            assert!(!request.headers().contains_key("x-api-key"));
            assert!(request.uri().query().is_none());
            response(StatusCode::OK, "{}")
        })
    });
    let return_url = format!("{}/finish", provider.endpoint());
    external.handle(move |request| {
        assert!(!request.headers().contains_key("x-api-key"));
        assert!(request.uri().query().is_none());
        Ok(redirect(&return_url))
    });
    authenticated_connection(&provider)
        .json("start", &[])
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(external.requests().len(), 1);
}

#[tokio::test]
async fn http_failures_preserve_retry_metadata() {
    for (status, header) in [
        (StatusCode::TOO_MANY_REQUESTS, "120"),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Wed, 21 Oct 2015 07:28:00 GMT",
        ),
        (StatusCode::TOO_MANY_REQUESTS, "invalid-delay"),
    ] {
        let fixture = HttpFixture::new();
        fixture.handle(move |_| {
            Ok(Response::builder()
                .status(status)
                .header("retry-after", header)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        });
        let error = connection(&fixture).json("limited", &[]).await.unwrap_err();
        assert_eq!(error.http_status(), Some(status.as_u16()));
        assert_eq!(error.retry_after(), Some(header));
        assert_eq!(fixture.requests().len(), 1);
    }
    let fixture = HttpFixture::new();
    fixture.respond(
        "/limited",
        StatusCode::TOO_MANY_REQUESTS,
        Buffer::new().to_bytes(),
    );
    let error = connection(&fixture).json("limited", &[]).await.unwrap_err();
    assert_eq!(error.http_status(), Some(429));
    assert_eq!(error.retry_after(), None);
}
