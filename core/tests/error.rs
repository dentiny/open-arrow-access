use open_arrow_access_core::ErrorKind;

// Registered failure statuses from the IANA HTTP status code registry.
// Unused 418 is covered by the unknown-client-error contract below.
const HTTP_FAILURES: &[(u16, ErrorKind)] = &[
    (400, ErrorKind::InvalidInput),
    (401, ErrorKind::Unauthenticated),
    (402, ErrorKind::PermissionDenied),
    (403, ErrorKind::PermissionDenied),
    (404, ErrorKind::NotFound),
    (405, ErrorKind::Unsupported),
    (406, ErrorKind::Unsupported),
    (407, ErrorKind::Unauthenticated),
    (408, ErrorKind::Timeout),
    (409, ErrorKind::Conflict),
    (410, ErrorKind::NotFound),
    (411, ErrorKind::InvalidInput),
    (412, ErrorKind::ConditionNotMet),
    (413, ErrorKind::InvalidInput),
    (414, ErrorKind::InvalidInput),
    (415, ErrorKind::Unsupported),
    (416, ErrorKind::InvalidInput),
    (417, ErrorKind::InvalidInput),
    (421, ErrorKind::Unavailable),
    (422, ErrorKind::InvalidInput),
    (423, ErrorKind::Conflict),
    (424, ErrorKind::Io),
    (425, ErrorKind::Unavailable),
    (426, ErrorKind::Unsupported),
    (428, ErrorKind::ConditionNotMet),
    (429, ErrorKind::RateLimited),
    (431, ErrorKind::InvalidInput),
    (451, ErrorKind::PermissionDenied),
    (500, ErrorKind::Io),
    (501, ErrorKind::Unsupported),
    (502, ErrorKind::Unavailable),
    (503, ErrorKind::Unavailable),
    (504, ErrorKind::Timeout),
    (505, ErrorKind::Unsupported),
    (506, ErrorKind::Io),
    (507, ErrorKind::ResourceExhausted),
    (508, ErrorKind::Io),
    (510, ErrorKind::Unsupported),
    (511, ErrorKind::Unauthenticated),
];

#[test]
fn registered_http_failures_have_specific_categories() {
    for &(status, expected) in HTTP_FAILURES {
        assert_eq!(
            ErrorKind::from_http_status(status),
            expected,
            "HTTP {status}"
        );
    }
}

#[test]
fn unknown_and_non_error_statuses_have_defined_fallbacks() {
    for status in [418, 419, 420, 427, 430, 432, 450, 452, 499] {
        assert_eq!(
            ErrorKind::from_http_status(status),
            ErrorKind::InvalidInput,
            "HTTP {status}"
        );
    }
    for status in [0, 99, 100, 200, 204, 302, 399, 509, 512, 599, 600, u16::MAX] {
        assert_eq!(
            ErrorKind::from_http_status(status),
            ErrorKind::Io,
            "HTTP {status}"
        );
    }
}
