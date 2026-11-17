use std::{error::Error as _, num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use futures::{Stream, StreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, Error, ErrorKind, ObjectRef, OrderRequirement, RecordCapability,
    RecordMetadata, RecordReader, Result, StreamReader, async_trait,
};
use open_arrow_access_layers::PrefetchLayer;
use tokio::sync::{Semaphore, oneshot};

fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn batch(rows: usize) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int32Array::from_iter_values(0..rows as i32)) as ArrayRef,
    )])
    .unwrap()
}

struct Source;

#[async_trait]
impl Accessor for Source {
    type Request = Box<dyn RecordReader>;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, reader: Self::Request) -> Result<Box<dyn RecordReader>> {
        Ok(reader)
    }
}

fn input(
    stream: impl Stream<Item = Result<RecordBatch>> + Send + 'static,
) -> Box<dyn RecordReader> {
    Box::new(StreamReader::records(
        stream,
        RecordMetadata {
            schema: batch(1).schema(),
            source: Some(ObjectRef::new("input")),
        },
        OrderRequirement::PreserveGlobal,
    ))
}

#[rstest::rstest]
#[case::rows(PrefetchLayer::rows(n(4)), 3)]
#[case::bytes(PrefetchLayer::estimated_bytes(n(batch(2).get_array_memory_size())), 2)]
#[case::oversized_rows(PrefetchLayer::rows(n(1)), 2)]
#[case::oversized_bytes(PrefetchLayer::estimated_bytes(n(1)), 2)]
#[case::both_bytes_first(
    PrefetchLayer::rows(n(100)).with_max_estimated_bytes(n(batch(2).get_array_memory_size())),
    2,
)]
#[case::both_rows_first(
    PrefetchLayer::estimated_bytes(n(batch(2).get_array_memory_size() * 100)).with_max_rows(n(4)),
    3,
)]
#[tokio::test]
async fn reads_ahead_to_the_first_enabled_budget_and_refills_on_consumption(
    #[case] layer: PrefetchLayer,
    #[case] depth: u32,
) {
    let produced = Arc::new(Semaphore::new(0));
    let signal = produced.clone();
    let value = batch(2);
    let stream = stream::iter((0..12).map(move |_| Ok(value.clone())))
        .inspect(move |_| signal.add_permits(1));
    let mut reader = Source.layer(layer).open(input(stream)).await.unwrap();
    reader.next().await.unwrap().unwrap();
    // Current-thread scheduling lets the worker run until its next suspension.
    produced.acquire_many(depth).await.unwrap().forget();
    assert_eq!(produced.available_permits(), 0);
    reader.next().await.unwrap().unwrap();
    produced.acquire().await.unwrap().forget();
    assert_eq!(produced.available_permits(), 0);
    let mut count = 2;
    while reader.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 12);
}

#[tokio::test]
async fn uneven_batches_wait_for_admission_without_further_reads() {
    let produced = Arc::new(Semaphore::new(0));
    let signal = produced.clone();
    let expected = [batch(1), batch(3), batch(3), batch(2)];
    let stream =
        stream::iter(expected.clone().into_iter().map(Ok)).inspect(move |_| signal.add_permits(1));
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(4)))
        .open(input(stream))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), expected[0]);
    // Three rows are queued; the next three-row batch must wait outside the
    // queue. It must not be admitted and allow the fourth read to start.
    produced.acquire_many(3).await.unwrap().forget();
    assert_eq!(produced.available_permits(), 0);
    for expected in &expected[1..] {
        assert_eq!(&reader.next().await.unwrap().unwrap(), expected);
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[rstest::rstest]
#[case(ErrorKind::Io)]
#[case(ErrorKind::Decode)]
#[tokio::test]
async fn source_errors_keep_order_context_and_source_and_are_terminal(#[case] kind: ErrorKind) {
    let error = Error::new(kind, "upstream", "read failed")
        .with_source(std::io::Error::other("original failure"));
    let stream = stream::iter([Ok(batch(1)), Ok(batch(2)), Err(error), Ok(batch(3))]);
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(100)))
        .open(input(stream))
        .await
        .unwrap();
    for rows in [1, 2] {
        assert_eq!(reader.next().await.unwrap().unwrap(), batch(rows));
    }
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "upstream");
    assert_eq!(error.source().unwrap().to_string(), "original failure");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn worker_panic_is_internal_after_queued_batches_and_is_terminal() {
    let panic = stream::poll_fn(|_| -> std::task::Poll<Option<Result<RecordBatch>>> {
        panic!("prefetch worker panic");
    });
    let stream = stream::iter([Ok(batch(1)), Ok(batch(2))]).chain(panic);
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(100)))
        .open(input(stream))
        .await
        .unwrap();
    for rows in [1, 2] {
        assert_eq!(reader.next().await.unwrap().unwrap(), batch(rows));
    }
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Internal);
    assert_eq!(error.operation(), "prefetch");
    let joined = error
        .source()
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap();
    assert!(joined.is_panic());
    assert!(joined.to_string().contains("prefetch worker panic"));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn dropping_the_reader_aborts_a_pending_upstream_read() {
    let (start, started) = oneshot::channel();
    let (lifetime, dropped) = oneshot::channel::<()>();
    let blocked = stream::once(async move {
        let _lifetime = lifetime;
        start.send(()).unwrap();
        std::future::pending::<Result<RecordBatch>>().await
    });
    let stream = stream::iter([Ok(batch(1))]).chain(blocked);
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(5)))
        .open(input(stream))
        .await
        .unwrap();
    reader.next().await.unwrap().unwrap();
    started.await.unwrap();
    drop(reader);
    assert!(dropped.await.is_err());
}
