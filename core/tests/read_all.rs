use futures::{StreamExt, stream};
use open_arrow_access_core::{
    ByteMetadata, ByteReadRequest, Bytes, ErrorKind, StreamReader, read_all_limited,
    read_object_limited,
};
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn limited_reads_accept_exact_size_and_stop_on_actual_chunk_overflow() {
    let source = MemoryAccessor::new(b"abcd", 2);
    assert_eq!(
        read_object_limited(&source, ByteReadRequest::new("object"), "test", 4)
            .await
            .unwrap(),
        b"abcd"
    );
    let chunks = stream::iter([Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))])
        .chain(stream::once(async {
            panic!("read after the byte limit was exceeded")
        }));
    let reader = StreamReader::new(chunks, ByteMetadata::default());
    assert_eq!(
        read_all_limited(Box::new(reader), "test", 3)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ResourceExhausted
    );
}
