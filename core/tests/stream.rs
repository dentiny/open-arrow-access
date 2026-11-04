mod common;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures::{StreamExt, TryStreamExt};
use open_arrow_access_core::{ErrorKind, into_stream};

use common::FailingReader;

#[tokio::test]
async fn stream_stops_after_error_without_polling_reader_again() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut stream = into_stream(Box::new(FailingReader::new(calls.clone())));
    assert_eq!(stream.try_next().await.err().unwrap().kind(), ErrorKind::Io);
    assert!(stream.next().await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
