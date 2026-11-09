use futures::TryStreamExt;
use http::{Response, StatusCode};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadOptions, Bytes, ErrorKind, ListRequest,
};
use open_arrow_access_service::remote::Connection;
use open_arrow_access_test_support::http::HttpFixture;
use opendal::{Buffer, HttpBody};

mod providers;
use providers::connection;
#[tokio::test]
async fn raw_json_and_lazy_signed_downloads() {
    let f = HttpFixture::new();
    f.respond(
        "/page?q=a%2Bb",
        StatusCode::OK,
        Bytes::from_static(b" { \"records\": [], \"unknown\": true } "),
    );
    f.file("/paper%20one?sig=a%2Bb%2Fz", Bytes::from_static(b"1234567"));
    let c = connection(&f);
    let doc = c.json("page", &[("q", "a+b")]).await.unwrap();
    assert!(doc.bytes().starts_with(b" { "));
    assert_eq!(doc.value()["unknown"], true);
    let a = c
        .files(
            vec![(
                "one".into(),
                format!("{}/paper%20one?sig=a%2Bb%2Fz", f.endpoint()),
            )],
            doc,
        )
        .unwrap();
    assert_eq!(f.requests().len(), 1);
    assert_eq!(
        a.list(ListRequest::new(""))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(!a.capability().read_with_range);
    assert!(a.open("missing".into()).await.is_err());
    open_arrow_access_test_support::byte_accessor::assert_read_contract(
        a,
        "one".into(),
        b"1234567",
    )
    .await
    .unwrap();
    assert_eq!(f.requests().len(), 3, "one page plus two GETs, no HEAD");
}
#[tokio::test]
async fn redirects_strip_credentials_and_keep_exact_signatures() {
    let f = HttpFixture::new();
    let dest = HttpFixture::new();
    let location = format!("{}/file?sig=x%2By", dest.endpoint());
    f.handle(move |req| {
        assert_eq!(req.headers()["x-api-key"], "secret");
        Ok(Response::builder()
            .status(302)
            .header("location", &location)
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    dest.file("/file?sig=x%2By", Bytes::from_static(b"ok"));
    let c = connection(&f)
        .with_header("x-api-key", "secret")
        .unwrap()
        .with_query("api_key", "hidden");
    let mut r = c
        .open(
            &format!("{}/start", f.endpoint()),
            ByteReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(r.next().await.unwrap().unwrap(), b"ok"[..]);
    assert!(!dest.requests()[0].headers.contains_key("x-api-key"));
    assert!(!format!("{c:?}").contains("secret"));
    assert!(!format!("{c:?}").contains("hidden"));
}
#[tokio::test]
async fn status_and_truncation_are_terminal() {
    let f = HttpFixture::new();
    let c = connection(&f);
    for (status, kind) in [
        (StatusCode::FORBIDDEN, ErrorKind::PermissionDenied),
        (StatusCode::NOT_FOUND, ErrorKind::NotFound),
        (StatusCode::TOO_MANY_REQUESTS, ErrorKind::Io),
    ] {
        f.respond("/bad", status, Bytes::new());
        assert_eq!(c.json("bad", &[]).await.unwrap_err().kind(), kind);
    }
    f.handle(|_| {
        Ok(Response::builder()
            .status(200)
            .header("content-length", "9")
            .body(HttpBody::new(
                futures::stream::iter([Ok(Buffer::from("short"))]),
                None,
            ))
            .unwrap())
    });
    let mut r = c
        .open(
            &format!("{}/short", f.endpoint()),
            ByteReadOptions::default(),
        )
        .await
        .unwrap();
    assert!(r.next().await.unwrap().is_some());
    assert_eq!(r.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert!(r.next().await.unwrap().is_none());
}
#[test]
fn invalid_endpoints_names_and_paths() {
    for endpoint in [
        "file:///x",
        "https://u:p@example.com",
        "https://example.com/path",
        "https://example.com?q=secret",
    ] {
        assert!(Connection::new(endpoint).is_err());
    }
    let c = Connection::new("https://example.com").unwrap();
    for path in ["//elsewhere", "../secret", "page?key=x"] {
        assert!(c.url(path, &[]).is_err());
    }
    assert!(c.files(vec![("x".into(), "file:///x".into())], ()).is_err());
}

#[tokio::test]
async fn scoped_query_survives_same_origin_redirect_without_duplicate_keys() {
    let f = HttpFixture::new();
    f.handle(|request| {
        let target = request.uri().to_string();
        assert!(target.ends_with("api_key=secret"));
        assert_eq!(target.matches("api_key=").count(), 1);
        let response = if request.uri().path() == "/start" {
            Response::builder()
                .status(302)
                .header("location", "/finish")
        } else {
            Response::builder().status(200)
        };
        Ok(response
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    let c = connection(&f).with_query("api_key", "secret");
    let url = format!("{}/start?api_key=secret", f.endpoint());
    let mut reader = c.open(&url, ByteReadOptions::default()).await.unwrap();
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(f.requests().len(), 2);
}
#[tokio::test]
async fn retains_retry_after_in_shared_error_source() {
    use open_arrow_access_service::remote::HttpStatusError;
    use std::error::Error as _;
    let f = HttpFixture::new();
    f.handle(|_| {
        Ok(Response::builder()
            .status(429)
            .header("retry-after", "120")
            .body(HttpBody::new(futures::stream::empty(), Some(0)))
            .unwrap())
    });
    let error = connection(&f).json("page", &[]).await.unwrap_err();
    let status = error
        .source()
        .unwrap()
        .downcast_ref::<HttpStatusError>()
        .unwrap();
    assert_eq!(status.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(status.retry_after.as_deref(), Some("120"));
}
