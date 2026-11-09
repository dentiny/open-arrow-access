use http::{Response, StatusCode};
use open_arrow_access_core::ErrorKind;
use open_arrow_access_service::{HttpStatusError, zenodo::ZenodoService};
use open_arrow_access_test_support::http::HttpFixture;
use opendal::HttpBody;
use std::{error::Error as _, num::NonZeroU64};
#[tokio::test]
async fn existing_provider_preserves_status_retry_after_and_error_category() {
    for (status, kind) in [
        (StatusCode::NOT_FOUND, ErrorKind::NotFound),
        (StatusCode::FORBIDDEN, ErrorKind::PermissionDenied),
        (StatusCode::TOO_MANY_REQUESTS, ErrorKind::Io),
    ] {
        let f = HttpFixture::new();
        f.handle(move |_| {
            Ok(Response::builder()
                .status(status)
                .header("retry-after", "120")
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        });
        let error = ZenodoService::with_endpoint(f.endpoint())
            .unwrap()
            .record(NonZeroU64::new(1).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<HttpStatusError>()
            .unwrap();
        assert_eq!(source.status, status);
        assert_eq!(source.retry_after.as_deref(), Some("120"));
        assert_eq!(f.requests().len(), 1);
    }
}
