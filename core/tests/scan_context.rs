use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures::{StreamExt, stream};
use open_arrow_access_core::*;
use tokio::sync::oneshot;

fn metadata() -> RecordMetadata {
    RecordMetadata {
        schema: Arc::new(Schema::empty()),
        source: None,
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_before_open_does_not_poll_accessor() {
    struct NeverOpen;
    #[async_trait]
    impl Accessor for NeverOpen {
        type Request = ();
        type Reader = dyn RecordReader;
        fn capability(&self) -> RecordCapability {
            RecordCapability::sequential(OrderRequirement::PreserveGlobal)
        }
        async fn open(&self, _: ()) -> Result<Box<dyn RecordReader>> {
            panic!("cancelled accessor was polled")
        }
    }
    let context = ScanContext::new();
    context.cancel();
    assert_eq!(
        context.open(&NeverOpen, ()).await.err().unwrap().kind(),
        ErrorKind::Cancelled
    );
}

#[tokio::test]
async fn all_pending_operations_wake_and_drop_their_state() {
    let context = ScanContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    let mut started = Vec::new();
    for _ in 0..8 {
        let context = context.clone();
        let guard = DropCount(drops.clone());
        let (ready, waiting) = oneshot::channel();
        started.push(waiting);
        tasks.spawn(async move {
            context
                .run(async move {
                    let _guard = guard;
                    ready.send(()).unwrap();
                    pending::<Result<()>>().await
                })
                .await
        });
    }
    for ready in started {
        ready.await.unwrap();
    }
    context.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap().unwrap_err().kind(), ErrorKind::Cancelled);
        }
    })
    .await
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn cancelling_pending_next_drops_reader_and_is_terminal() {
    let context = ScanContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let guard = DropCount(drops.clone());
    let (started, ready) = oneshot::channel();
    let batches = stream::once(async move {
        let _guard = guard;
        started.send(()).unwrap();
        pending::<Result<RecordBatch>>().await
    });
    let source = StreamReader::records(batches, metadata(), OrderRequirement::PreserveGlobal);
    let mut reader = context.wrap_reader(Box::new(source));
    let cancel = async {
        ready.await.unwrap();
        context.cancel();
    };
    let (result, ()) = tokio::join!(reader.next(), cancel);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Cancelled);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(reader.metadata().schema, metadata().schema);
}

#[tokio::test]
async fn deadline_is_shared_across_preparation_and_consumption() {
    let (expire, deadline) = oneshot::channel();
    let context = ScanContext::new().with_deadline(async {
        let _ = deadline.await;
    });
    assert_eq!(context.run(async { Ok(7) }).await.unwrap(), 7);
    let mut reader = context.wrap_reader(Box::new(StreamReader::records(
        stream::pending(),
        metadata(),
        OrderRequirement::PreserveGlobal,
    )));
    let (result, ()) = tokio::join!(reader.next(), async {
        expire.send(()).unwrap();
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Timeout);
    assert_eq!(
        context.run(async { Ok(8) }).await.unwrap_err().kind(),
        ErrorKind::Timeout
    );
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn source_errors_drop_reader_without_waiting_for_cancellation() {
    let context = ScanContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let guard = DropCount(drops.clone());
    let batches =
        stream::once(async { Err(Error::new(ErrorKind::Decode, "fixture", "bad record")) }).chain(
            stream::once(async move {
                let _guard = guard;
                pending().await
            }),
        );
    let mut reader = context.wrap_reader(Box::new(StreamReader::records(
        batches,
        metadata(),
        OrderRequirement::PreserveGlobal,
    )));
    assert_eq!(reader.next().await.unwrap_err().kind(), ErrorKind::Decode);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(reader.next().await.unwrap().is_none());
}
