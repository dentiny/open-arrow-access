use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use futures::{channel::oneshot, stream};
use open_arrow_access_core::*;
use open_arrow_access_test_support::DropProbe;

fn metadata() -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    }
}

#[tokio::test]
async fn cancelled_operations_are_not_polled_and_all_pending_waiters_wake() {
    let cancelled = ReadCancellation::new();
    cancelled.cancel();
    assert_eq!(
        cancelled
            .run::<()>(async { panic!("cancelled open was polled") })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Cancelled
    );

    let cancellation = ReadCancellation::new();
    let mut tasks = Vec::new();
    let mut started = Vec::new();
    for _ in 0..2 {
        let cancellation = cancellation.clone();
        let (ready, waiting) = oneshot::channel();
        started.push(waiting);
        tasks.push(tokio::spawn(async move {
            cancellation
                .run(async move {
                    ready.send(()).unwrap();
                    pending::<Result<()>>().await
                })
                .await
        }));
    }
    for ready in started {
        ready.await.unwrap();
    }
    cancellation.cancel();
    cancellation.cancel();
    for task in tasks {
        assert_eq!(
            task.await.unwrap().unwrap_err().kind(),
            ErrorKind::Cancelled
        );
    }
}

#[tokio::test]
async fn cancelling_pending_next_releases_reader_and_preserves_metadata() {
    let cancellation = ReadCancellation::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe::new(dropped.clone());
    let (started, ready) = oneshot::channel();
    let batches = stream::once(async move {
        let _probe = probe;
        started.send(()).unwrap();
        pending::<Result<RecordBatch>>().await
    });
    let source = StreamReader::records(batches, metadata(), OrderRequirement::PreserveGlobal);
    let mut reader = cancellation.wrap_reader(Box::new(source));
    let schema = reader.metadata().schema.clone();
    let (result, ()) = tokio::join!(reader.next(), async {
        ready.await.unwrap();
        cancellation.cancel();
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(Arc::ptr_eq(&reader.metadata().schema, &schema));
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn upstream_errors_remain_unchanged_and_terminal() {
    let error = Error::new(ErrorKind::PermissionDenied, "fixture", "access denied")
        .with_source(std::io::Error::other("original cause"));
    let source = StreamReader::records(
        stream::iter([Err(error)]),
        metadata(),
        OrderRequirement::PreserveGlobal,
    );
    let mut reader = ReadCancellation::new().wrap_reader(Box::new(source));
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.operation(), "fixture");
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        "original cause"
    );
    assert!(reader.next().await.unwrap().is_none());
}
