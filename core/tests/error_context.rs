use std::{error::Error as _, io};

use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

#[test]
fn context_preserves_error_identity_and_metadata() {
    let error = Error::new(ErrorKind::Unavailable, "decode", "upstream failed")
        .with_source(io::Error::new(io::ErrorKind::ConnectionReset, "reset"))
        .with_http_response(503, Some("2".into()))
        .with_object(Some(&ObjectRef::new("s3://bucket/b.warc.gz")))
        .with_object(Some(&ObjectRef::new("outer-directory")));
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.operation(), "decode");
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.retry_after(), Some("2"));
    assert!(error.to_string().contains("s3://bucket/b.warc.gz"));
    assert!(!error.to_string().contains("outer-directory"));
    assert_eq!(error.to_string().matches("object ").count(), 1);
    assert_eq!(error.source().unwrap().to_string(), "reset");
}
