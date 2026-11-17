use futures::io::{AsyncBufReadExt, AsyncReadExt};
use open_arrow_access_core::{
    Accessor, AccessorExt, BytePosition, into_positioned_async_read, metrics::MetricsLayer,
};
use open_arrow_access_test_support::MemoryAccessor;

#[tokio::test]
async fn buffered_position_tracks_consumption_and_ranges_do_not_move_storage_cursor() {
    let input = MemoryAccessor::new(b"abcdefghij", 4)
        .with_ranges()
        .layer(MetricsLayer::default());
    let mut reader = input.open("object".into()).await.unwrap();
    assert_eq!(reader.position(), Some(BytePosition::Stored(0)));
    assert_eq!(reader.read_range(5..8).await.unwrap().as_ref(), b"fgh");
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
    let frame = BytePosition::Frame {
        frame: 99,
        decoded: 12,
    }
    .advance(3);
    assert_eq!((frame.offset(), frame.offset_in_frame()), (99, 15));
}
