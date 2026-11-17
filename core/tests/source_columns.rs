use futures::io::{AsyncBufReadExt, AsyncReadExt};
use open_arrow_access_core::{
    Accessor, AccessorExt, BytePosition, into_positioned_async_read, metrics::MetricsLayer,
};
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn buffered_position_tracks_consumption_and_marks_chunk_boundaries() {
    let input = MemoryAccessor::new(b"abcdefghij", 4).layer(MetricsLayer::default());
    let reader = input.open("object".into()).await.unwrap();
    assert_eq!(reader.position(), Some(BytePosition::Stored(0)));
    let mut input = into_positioned_async_read(reader, "test");
    assert_eq!(input.fill_buf().await.unwrap(), b"abcd");
    assert_eq!(input.position(), Some(BytePosition::Stored(0)));
    input.consume_unpin(3);
    assert_eq!(input.position(), Some(BytePosition::Stored(3)));
    assert_eq!(input.fill_buf().await.unwrap(), b"d");
    let observer = input.observer();
    input.consume_unpin(1);
    observer.mark();
    let mut bytes = [0; 3];
    input.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"efg");
    assert_eq!(observer.marked_position(), Some(BytePosition::Stored(4)));
    assert_eq!(input.position(), Some(BytePosition::Stored(7)));
}

#[tokio::test]
async fn pending_fetch_is_retained_until_ready() {
    use open_arrow_access_core::{ByteMetadata, Bytes, StreamReader};
    use std::task::Poll;

    let mut pending = true;
    let mut bytes = Some(Bytes::from_static(b"abc"));
    let stream = futures::stream::poll_fn(move |cx| {
        if std::mem::take(&mut pending) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(bytes.take().map(Ok))
    });
    let mut input = into_positioned_async_read(
        Box::new(StreamReader::new(stream, ByteMetadata::default())),
        "test",
    );
    let mut first = Box::pin(input.fill_buf());
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert_eq!(first.await.unwrap(), b"abc");
    input.consume_unpin(3);
    assert!(input.fill_buf().await.unwrap().is_empty());
}
