use http::Response;
use open_arrow_access_test_support::http::HttpFixture;
use opendal::{Buffer, HttpBody};
mod providers;
use providers::connection;

#[tokio::test]
async fn buffered_and_streaming_reads_share_length_validation() {
    let raw = open_arrow_access_test_support::fixture!("zenodo", "empty.json").await;
    for declared in [
        None,
        Some(raw.len()),
        Some(raw.len() - 1),
        Some(raw.len() + 1),
    ] {
        let fixture = HttpFixture::new();
        let body = raw.clone();
        fixture.handle(move |_| {
            let mut response = Response::builder().status(200);
            if let Some(length) = declared {
                response = response.header("content-length", length);
            }
            Ok(response
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from(body.clone()))]),
                    None,
                ))
                .unwrap())
        });
        for buffered in [true, false] {
            let connection = connection(&fixture);
            let result = if buffered {
                connection.json("data", &[]).await.map(|_| ())
            } else {
                let input = connection
                    .open(&format!("{}/data", fixture.endpoint()), Default::default())
                    .await;
                match input {
                    Ok(input) => open_arrow_access_core::read_all(input, "test")
                        .await
                        .map(|_| ()),
                    Err(error) => Err(error),
                }
            };
            if declared.is_none_or(|length| length == raw.len()) {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().kind(),
                    open_arrow_access_core::ErrorKind::Decode
                );
            }
        }
    }
}

#[cfg(feature = "zenodo")]
#[tokio::test]
async fn metadata_rejects_incorrect_declared_body_lengths() {
    use open_arrow_access_core::ErrorKind;
    use open_arrow_access_service::zenodo::ZenodoService;
    use open_arrow_access_test_support::fixture;
    let raw = fixture!("zenodo", "empty.json").await;
    for declared in [raw.len() - 1, raw.len() + 1] {
        let fixture = HttpFixture::new();
        let body = raw.clone();
        fixture.handle(move |_| {
            Ok(http::Response::builder()
                .status(200)
                .header("content-length", declared)
                .body(opendal::HttpBody::new(
                    futures::stream::iter([Ok(opendal::Buffer::from(body.clone()))]),
                    None,
                ))
                .unwrap())
        });
        let error = ZenodoService::with_endpoint(fixture.endpoint())
            .unwrap()
            .record(std::num::NonZeroU64::new(3551612).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Decode);
        assert_eq!(error.operation(), "resolve_zenodo");
    }
}
