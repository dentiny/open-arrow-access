use open_arrow_access_core::ErrorKind;

#[test]
fn http_statuses_map_to_shared_error_categories() {
    for (status, kind) in [
        (400, ErrorKind::InvalidInput),
        (401, ErrorKind::Unauthenticated),
        (403, ErrorKind::PermissionDenied),
        (404, ErrorKind::NotFound),
        (405, ErrorKind::Unsupported),
        (408, ErrorKind::Timeout),
        (409, ErrorKind::Conflict),
        (412, ErrorKind::ConditionNotMet),
        (429, ErrorKind::RateLimited),
        (500, ErrorKind::Io),
        (503, ErrorKind::Unavailable),
        (507, ErrorKind::ResourceExhausted),
    ] {
        assert_eq!(ErrorKind::from_http_status(status), kind, "HTTP {status}");
    }
}
