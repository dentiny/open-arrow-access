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
