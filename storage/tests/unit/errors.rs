use open_arrow_access_core::ErrorKind;

#[test]
fn backend_error_categories_keep_the_upstream_cause() {
    use std::error::Error as _;

    for (upstream, expected) in [
        (opendal::ErrorKind::NotFound, ErrorKind::NotFound),
        (
            opendal::ErrorKind::PermissionDenied,
            ErrorKind::PermissionDenied,
        ),
        (opendal::ErrorKind::Unsupported, ErrorKind::Unsupported),
        (opendal::ErrorKind::Unexpected, ErrorKind::Io),
        (opendal::ErrorKind::RateLimited, ErrorKind::Io),
    ] {
        let source = opendal::Error::new(upstream, "backend request failed").set_source(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"),
        );
        let error = crate::backend::map_error("read", "fixture.parquet", source);
        assert_eq!(error.kind(), expected, "{upstream:?}");
        assert_eq!(error.operation(), "read");
        let cause = error
            .source()
            .unwrap()
            .downcast_ref::<opendal::Error>()
            .unwrap();
        assert_eq!(cause.kind(), upstream);
        assert_eq!(
            cause
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::ConnectionReset
        );
    }
}
