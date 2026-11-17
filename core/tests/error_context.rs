use std::{error::Error as _, io};

use open_arrow_access_core::{Error, ErrorKind, ObjectRef};

#[tokio::test]
async fn context_preserves_error_identity_and_metadata() {
    let error = Error::new(ErrorKind::Unavailable, "decode", "upstream failed")
        .with_source(io::Error::new(io::ErrorKind::ConnectionReset, "reset"))
        .with_http_response(503, Some("2".into()))
        .with_object(None)
        .with_object(Some(&ObjectRef::new("s3://bucket/b.warc.gz")))
        .with_object(Some(&ObjectRef::new("outer-directory")))
        .with_object(None);
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.operation(), "decode");
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.retry_after(), Some("2"));
    assert_eq!(
        error.to_string(),
        r#"decode: object "s3://bucket/b.warc.gz": upstream failed"#
    );
    assert_eq!(error.source().unwrap().to_string(), "reset");
}

#[tokio::test]
async fn missing_object_keeps_original_display() {
    let error = Error::from_source(
        ErrorKind::Decode,
        "decode",
        io::Error::new(io::ErrorKind::InvalidData, "invalid data"),
    )
    .with_object(None);
    assert_eq!(error.to_string(), "decode: invalid data");
}
