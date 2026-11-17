use http::{Request, StatusCode};
use open_arrow_access_test_support::http::HttpFixture;
use opendal::{Buffer, ErrorKind, OperationContext};

fn request(url: &str) -> Request<Buffer> {
    Request::builder().uri(url).body(Buffer::default()).unwrap()
}

#[tokio::test]
async fn fixtures_leave_the_process_default_transport_uninstalled() {
    // This test binary installs no global transport. Creating a fixture must
    // not turn an unrelated default context into a fixture-backed client.
    let context = OperationContext::new();
    let before = context
        .http_transport()
        .send(request("https://unrelated.invalid/"))
        .await
        .unwrap_err();
    assert_eq!(before.kind(), ErrorKind::ConfigInvalid);
    let fixture = HttpFixture::new();
    fixture.file("/data", "fixture");
    let after = OperationContext::new()
        .http_transport()
        .send(request("https://unrelated.invalid/"))
        .await
        .unwrap_err();
    assert_eq!(after.kind(), ErrorKind::ConfigInvalid);
    assert!(fixture.requests().is_empty());
}

#[tokio::test]
async fn explicit_contexts_route_live_fixtures_and_fail_unknown_or_expired_hosts() {
    let first = HttpFixture::new();
    let second = HttpFixture::new();
    first.respond("/page", StatusCode::OK, "first");
    second.respond("/page", StatusCode::OK, "second");
    let context = first.context();
    // One explicitly injected context can follow a provider redirect to a
    // second fixture without installing a process-wide transport.
    for (fixture, expected) in [(&first, b"first".as_slice()), (&second, b"second")] {
        let response = context
            .http_transport()
            .send(request(&format!("{}/page", fixture.endpoint())))
            .await
            .unwrap();
        assert_eq!(response.into_body().to_bytes().as_ref(), expected);
        assert_eq!(fixture.requests().len(), 1);
    }
    let error = context
        .http_transport()
        .send(request("https://unrelated.invalid/"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
    assert!(
        error
            .to_string()
            .contains("unknown host or expired HTTP fixture")
    );
    let expired_url = format!("{}/page", second.endpoint());
    drop(second);
    let error = context
        .http_transport()
        .send(request(&expired_url))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unexpected);
}
