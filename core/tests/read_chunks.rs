use futures::{TryStreamExt, io::Cursor};
use open_arrow_access_core::read_chunks;
use std::num::NonZeroUsize;

#[tokio::test]
async fn short_reads_do_not_retain_large_requested_allocations() {
    let chunks = read_chunks(
        Cursor::new(vec![7; 17]),
        NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
    );
    futures::pin_mut!(chunks);
    let bytes = chunks.try_next().await.unwrap().unwrap();
    assert_eq!(&bytes[..], &[7; 17]);
    let bytes = bytes.try_into_mut().unwrap();
    assert!(
        bytes.capacity() <= 64 * 1024,
        "short output retained {} bytes",
        bytes.capacity()
    );
    assert!(chunks.try_next().await.unwrap().is_none());
    assert!(chunks.try_next().await.unwrap().is_none());
}
