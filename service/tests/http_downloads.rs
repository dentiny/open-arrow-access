mod providers;
use http::StatusCode;
use open_arrow_access_core::{ByteReadOptions, Bytes, ErrorKind};
use open_arrow_access_test_support::{collect_bytes, http::HttpFixture};
#[tokio::test]
async fn get_only_unknown_length_stream_respects_chunk_size() {
    let f = HttpFixture::new();
    f.respond_chunked(
        "/file?sig=a%2Bb",
        StatusCode::OK,
        Bytes::from_static(b"1234567"),
    );
    let c = providers::connection(&f);
    let options = ByteReadOptions {
        io_chunk_size: std::num::NonZeroUsize::new(2),
        ..Default::default()
    };
    let mut reader = c
        .open(&format!("{}/file?sig=a%2Bb", f.endpoint()), options)
        .await
        .unwrap();
    assert_eq!(reader.metadata().content_length, None);
    assert!(!reader.capability().read_with_range);
    let mut data = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        assert!(chunk.len() <= 2);
        data.extend_from_slice(&chunk);
    }
    assert_eq!(data, b"1234567");
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(f.requests().len(), 1);
}
#[tokio::test]
async fn truncated_http_body_is_terminal() {
    use opendal::{Buffer, HttpBody};
    let f = HttpFixture::new();
    f.handle(|_| {
        Ok(http::Response::builder()
            .status(200)
            .header("content-length", "9")
            .body(HttpBody::new(
                futures::stream::iter([Ok(Buffer::from("short"))]),
                None,
            ))
            .unwrap())
    });
    let mut reader = providers::connection(&f)
        .open(
            &format!("{}/short", f.endpoint()),
            ByteReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        collect_bytes(reader.as_mut()).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert!(reader.next().await.unwrap().is_none());
}
