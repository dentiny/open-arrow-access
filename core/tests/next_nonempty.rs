use std::sync::atomic::{AtomicUsize, Ordering};

use futures::stream;
use open_arrow_access_core::{
    ByteMetadata, Bytes, Error, ErrorKind, StreamReader, empty_chunk, next_nonempty,
};

#[tokio::test]
async fn checks_one_chunk_without_skipping_or_eagerly_constructing_errors() {
    let failures = AtomicUsize::new(0);
    let mut reader = StreamReader::new(
        stream::iter([
            Ok(Bytes::from_static(b"first")),
            Ok(Bytes::new()),
            Ok(Bytes::from_static(b"must not skip to here")),
        ]),
        ByteMetadata::default(),
    );
    let failure = || {
        failures.fetch_add(1, Ordering::SeqCst);
        empty_chunk("consume_fixture")
    };
    assert_eq!(
        next_nonempty(&mut reader, failure).await.unwrap().unwrap(),
        "first"
    );
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    let error = next_nonempty(&mut reader, failure).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "consume_fixture");
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    // The caller stops after the contract failure; the helper does not skip it.
}

#[tokio::test]
async fn preserves_eof_and_source_errors_without_calling_empty_factory() {
    let mut eof = StreamReader::new(stream::empty(), ByteMetadata::default());
    assert!(
        next_nonempty(&mut eof, || panic!("EOF is not an empty chunk"))
            .await
            .unwrap()
            .is_none()
    );
    let mut failure = StreamReader::new(
        stream::once(async {
            Err::<Bytes, _>(Error::new(
                ErrorKind::PermissionDenied,
                "upstream",
                "denied",
            ))
        }),
        ByteMetadata::default(),
    );
    let error = next_nonempty(&mut failure, || panic!("source errors pass through"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "upstream");
    assert_eq!(error.to_string(), "upstream: denied");
}
