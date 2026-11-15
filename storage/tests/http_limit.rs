use futures::poll;
use http::{Request, StatusCode};
use open_arrow_access_storage::{
    HttpConcurrencyLimitLayer,
    opendal::{Buffer, HttpTransporter},
};
use open_arrow_access_test_support::http::HttpFixture;

#[tokio::test]
async fn cloned_limits_share_capacity_until_body_eof_or_drop() {
    let fixture = HttpFixture::new();
    fixture.respond("/file", StatusCode::OK, "data");
    let limit = HttpConcurrencyLimitLayer::new(1).unwrap();
    let a = limit.wrap(HttpTransporter::default());
    let b = limit.clone().wrap(HttpTransporter::default());
    let request = || {
        Request::get(format!("{}/file", fixture.endpoint()))
            .body(Buffer::new())
            .unwrap()
    };

    let mut first = a.fetch(request()).await.unwrap().into_body();
    let waiting = b.fetch(request());
    tokio::pin!(waiting);
    assert!(poll!(&mut waiting).is_pending());
    assert_eq!(fixture.requests().len(), 1);

    assert_eq!(first.to_buffer().await.unwrap().to_vec(), b"data");
    // EOF returns capacity even though `first` remains alive.
    let second = waiting.await.unwrap();
    drop(second);
    let _third = a.fetch(request()).await.unwrap();
    assert_eq!(fixture.requests().len(), 3);
}
