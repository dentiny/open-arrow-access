use std::{error::Error as _, io};

use futures::stream;
use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Reader, StreamReader};

#[tokio::test]
async fn context_preserves_error_identity_metadata_and_terminal_behavior() {
    let error = Error::new(ErrorKind::Unavailable, "decode", "upstream failed")
        .with_source(io::Error::new(io::ErrorKind::ConnectionReset, "reset"))
        .with_http_response(503, Some("2".into()))
        .with_object(&ObjectRef::new("s3://bucket/b.warc.gz"));
    let mut reader = StreamReader::new(stream::iter([Err::<u8, _>(error), Ok(1)]), ())
        .with_object(ObjectRef::new("outer-directory"));
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.operation(), "decode");
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.retry_after(), Some("2"));
    assert_eq!(error.object().unwrap().locator(), "s3://bucket/b.warc.gz");
    assert_eq!(error.to_string().matches("object ").count(), 1);
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::ConnectionReset
    );
    assert!(reader.next().await.unwrap().is_none());
}
