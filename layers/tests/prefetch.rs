use std::{error::Error as _, num::NonZeroUsize, sync::Arc};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use futures::{Stream, StreamExt, stream};
use open_arrow_access_core::{
    Accessor, AccessorExt, Error, ErrorKind, ObjectRef, OrderRequirement, ReadOptions, ReadRequest,
    RecordCapability, RecordMetadata, RecordReader, Result, StreamReader, async_trait,
};
use open_arrow_access_layers::{LimitLayer, PrefetchLayer};
use open_arrow_access_test_support::MemoryRecordAccessor;
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

#[tokio::test]
async fn opening_is_lazy_and_each_reader_preserves_batches_and_metadata() {
    let input = batch(3);
    let batches = vec![input.clone(), input.slice(0, 0), input.slice(1, 2)];
    let source = MemoryRecordAccessor::new(batches.clone());
    let polls = source.polls.clone();
    let accessor = source.layer(PrefetchLayer::rows(n(5)));
    let capability = accessor.capability();
    let first = accessor.open(ReadRequest::new("input")).await.unwrap();
    let second = accessor.open(ReadRequest::new("input")).await.unwrap();
    assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(first.capability(), capability);
    assert_eq!(first.metadata().source, Some(ObjectRef::new("input")));
    assert_eq!(first.metadata().schema, input.schema());
    for mut reader in [first, second] {
        for expected in &batches {
            let actual = reader.next().await.unwrap().unwrap();
            assert_eq!(&actual, expected);
            assert!(Arc::ptr_eq(actual.column(0), expected.column(0)));
        }
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
        assert_eq!(reader.metadata().schema, input.schema());
    }
}

#[tokio::test]
async fn reads_ahead_to_the_first_enabled_budget_and_refills_on_consumption() {
    let value = batch(2);
    let bytes = value.get_array_memory_size();
    // Both construction paths and both limits independently cause backpressure.
    for (layer, depth) in [
        (PrefetchLayer::rows(n(4)), 3),
        (PrefetchLayer::estimated_bytes(n(bytes)), 2),
        (
            PrefetchLayer::rows(n(100)).with_max_estimated_bytes(n(bytes)),
            2,
        ),
        (
            PrefetchLayer::estimated_bytes(n(bytes * 100)).with_max_rows(n(4)),
            3,
        ),
    ] {
        let produced = Arc::new(Semaphore::new(0));
        let signal = produced.clone();
        let value = value.clone();
        let stream = stream::iter((0..12).map(move |_| Ok(value.clone())))
            .inspect(move |_| signal.add_permits(1));
        let mut reader = Source.layer(layer).open(input(stream)).await.unwrap();
        assert_eq!(reader.capability(), Source.capability());
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

#[tokio::test]
async fn oversized_and_empty_batches_make_bounded_progress() {
    let large = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from(vec!["x".repeat(4096); 20])) as ArrayRef,
    )])
    .unwrap();
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    for (value, layer) in [
        (large.clone(), PrefetchLayer::rows(n(1))),
        (large, PrefetchLayer::estimated_bytes(n(1))),
        (empty.clone(), PrefetchLayer::rows(n(1))),
        (empty, PrefetchLayer::estimated_bytes(n(1))),
    ] {
        let produced = Arc::new(Semaphore::new(0));
        let signal = produced.clone();
        let source = MemoryRecordAccessor::new(vec![value.clone(); 10]);
        let inner = source.open(ReadRequest::new("input")).await.unwrap();
        let metadata = inner.metadata().clone();
        let batches = stream::unfold(inner, |mut inner| async move {
            inner.next().await.transpose().map(|item| (item, inner))
        })
        .inspect(move |_| signal.add_permits(1));
        let inner = Box::new(StreamReader::records(
            batches,
            metadata,
            OrderRequirement::Reorderable,
        )) as Box<dyn RecordReader>;
        let mut reader = Source.layer(layer).open(inner).await.unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap(), value);
        produced.acquire_many(2).await.unwrap().forget();
        assert_eq!(produced.available_permits(), 0);
        for _ in 1..10 {
            assert_eq!(reader.next().await.unwrap().unwrap(), value);
        }
        assert!(reader.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn zero_column_batches_retain_rows_under_a_byte_budget() {
    let source = MemoryRecordAccessor::new(vec![batch(5); 4]);
    let request = ReadRequest::new("input").with_options(ReadOptions {
        projection: Some(vec![]),
        ..ReadOptions::default()
    });
    let mut reader = source
        .layer(PrefetchLayer::estimated_bytes(n(1)))
        .open(request)
        .await
        .unwrap();
    for _ in 0..4 {
        let output = reader.next().await.unwrap().unwrap();
        assert_eq!(output.num_columns(), 0);
        assert_eq!(output.num_rows(), 5);
    }
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn source_errors_keep_order_context_and_source_and_are_terminal() {
    let error = Error::new(ErrorKind::Io, "upstream", "read failed")
        .with_source(std::io::Error::other("original failure"))
        .with_http_response(503, Some("10".into()));
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
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.operation(), "upstream");
    assert_eq!(error.http_status(), Some(503));
    assert_eq!(error.retry_after(), Some("10"));
    assert_eq!(error.source().unwrap().to_string(), "original failure");
    assert!(error.to_string().contains("input"));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn worker_panics_are_terminal_errors_after_queued_batches() {
    let stream = stream::iter([Ok(batch(1))]).chain(stream::poll_fn(|_| {
        panic!("reader panicked");
    }));
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(10)))
        .open(input(stream))
        .await
        .unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), batch(1));
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.operation(), "prefetch");
    assert!(error.to_string().contains("reader panicked"));
    assert!(error.source().is_some());
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn dropping_or_limiting_the_reader_aborts_a_pending_upstream_read() {
    for limited in [false, true] {
        let (start, started) = oneshot::channel();
        let (lifetime, dropped) = oneshot::channel::<()>();
        let blocked = stream::once(async move {
            let _lifetime = lifetime;
            start.send(()).unwrap();
            std::future::pending::<Result<RecordBatch>>().await
        });
        let stream = stream::iter([Ok(batch(1))]).chain(blocked);
        let accessor = Source.layer(PrefetchLayer::rows(n(5)));
        let mut reader = if limited {
            accessor.layer(LimitLayer::new(1)).open(input(stream)).await
        } else {
            accessor.open(input(stream)).await
        }
        .unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap(), batch(1));
        started.await.unwrap();
        if limited {
            assert!(reader.next().await.unwrap().is_none());
        }
        drop(reader);
        assert!(dropped.await.is_err());
    }
}

#[tokio::test]
async fn cancelling_the_first_next_drops_its_worker() {
    let (start, started) = oneshot::channel();
    let (lifetime, dropped) = oneshot::channel::<()>();
    let stream = stream::once(async move {
        let _lifetime = lifetime;
        start.send(()).unwrap();
        std::future::pending::<Result<RecordBatch>>().await
    });
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(5)))
        .open(input(stream))
        .await
        .unwrap();
    let consumer = tokio::spawn(async move { reader.next().await });
    started.await.unwrap();
    consumer.abort();
    assert!(consumer.await.unwrap_err().is_cancelled());
    assert!(dropped.await.is_err());
}

#[test]
fn no_runtime_is_needed_to_open_and_missing_runtime_is_reported_on_read() {
    futures::executor::block_on(async {
        let mut reader = Source
            .layer(PrefetchLayer::rows(n(5)))
            .open(input(stream::iter([Ok(batch(1))])))
            .await
            .unwrap();
        assert_eq!(reader.metadata().schema, batch(1).schema());
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), "prefetch");
        assert!(reader.next().await.unwrap().is_none());
    });
}

#[tokio::test]
async fn dropping_a_reader_aborts_a_worker_waiting_for_queue_space() {
    let (lifetime, dropped) = oneshot::channel::<()>();
    let produced = Arc::new(Semaphore::new(0));
    let signal = produced.clone();
    let batches = stream::unfold((lifetime, 0), |(lifetime, index)| async move {
        let rows = if index == 0 { 1 } else { 3 };
        Some((Ok(batch(rows)), (lifetime, index + 1)))
    })
    .inspect(move |_| signal.add_permits(1));
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(4)))
        .open(input(batches))
        .await
        .unwrap();
    reader.next().await.unwrap().unwrap();
    produced.acquire_many(3).await.unwrap().forget();
    assert_eq!(produced.available_permits(), 0);
    drop(reader);
    assert!(dropped.await.is_err());
}

#[tokio::test]
async fn empty_input_is_terminal_and_releases_its_reader() {
    let (lifetime, dropped) = oneshot::channel::<()>();
    let batches = stream::once(async move {
        let _lifetime = lifetime;
        None::<Result<RecordBatch>>
    })
    .filter_map(std::future::ready);
    let mut reader = Source
        .layer(PrefetchLayer::rows(n(4)))
        .open(input(batches))
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_none());
    assert!(dropped.await.is_err());
    assert!(reader.next().await.unwrap().is_none());
}
