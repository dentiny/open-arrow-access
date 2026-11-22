use super::*;
use open_arrow_access_core::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

fn page() -> BhlPageMetadata {
    let object = ObjectRef::new("ocr/item-2/item-2-3-5.txt");
    BhlPageMetadata {
        identity: crate::bhl::BhlPageIdentity::parse(object.locator()).unwrap(),
        object,
        metadata: crate::bhl::page::tests::metadata("title", "rights"),
        missing: vec![],
        conflicts: vec![],
    }
}

#[tokio::test]
async fn delivers_first_page_before_query_finishes_off_the_async_thread() {
    let async_thread = std::thread::current().id();
    let (release, wait) = std::sync::mpsc::channel();
    let mut query = Query::start(move |emit| {
        assert_ne!(std::thread::current().id(), async_thread);
        emit(page())?;
        wait.recv().unwrap();
        Err(error::invalid("late query failure"))
    });
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), query.next()).await;
    // Always release the worker, even if the incremental-read assertion fails.
    release.send(()).unwrap();
    assert_eq!(first.unwrap().unwrap().unwrap().identity.page_id, 3);
    let failure = query.next().await.err().unwrap();
    assert_eq!(failure.to_string(), "bhl_metadata: late query failure");
    assert!(query.next().await.unwrap().is_none());
}

#[tokio::test]
async fn bounds_queued_pages_and_stops_when_the_reader_is_dropped() {
    let emitted = Arc::new(AtomicUsize::new(0));
    let observed = emitted.clone();
    let (queued, ready) = oneshot::channel();
    let (finished, done) = oneshot::channel();
    let query = Query::start(move |emit| {
        emit(page())?;
        observed.fetch_add(1, Ordering::SeqCst);
        queued.send(()).unwrap();
        let result = emit(page()); // Cannot queue a second page without a consumer.
        if result.is_ok() {
            observed.fetch_add(1, Ordering::SeqCst);
        }
        finished.send(result.is_err()).unwrap();
        result
    });
    ready.await.unwrap();
    assert_eq!(emitted.load(Ordering::SeqCst), 1);
    drop(query);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(emitted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn propagates_worker_panics_instead_of_successful_eof() {
    let mut query = Query::start(|_| panic!("query worker panic"));
    let failure = query.next().await.err().unwrap();
    assert_eq!(failure.kind(), ErrorKind::Internal);
    assert!(query.next().await.unwrap().is_none());
}
