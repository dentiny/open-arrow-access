use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{UInt64Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, AccessorExt, Error, ErrorKind, RecordBatch, RecordMetadata, RecordReader,
    ScanPlanner, StreamReader,
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::common::PageSetAccessor;
use tokio::sync::{Notify, Semaphore};

fn page(value: u64, name: &str) -> Box<dyn RecordReader> {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::UInt64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![value]))],
    )
    .unwrap();
    Box::new(StreamReader::new(
        stream::iter([Ok(batch)]),
        RecordMetadata {
            schema,
            source: Some(format!("page-{value}").into()),
        },
    ))
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn independent_page_fetches_are_lazy_bounded_and_keep_selection_order() {
    let started = Arc::new(Mutex::new(Vec::new()));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let notified = Arc::new(Notify::new());
    let gates: Arc<Vec<_>> = Arc::new((0..3).map(|_| Semaphore::new(0)).collect());
    let pages = PageSetAccessor::new({
        let (started, active, peak, notified, gates) = (
            started.clone(),
            active.clone(),
            peak.clone(),
            notified.clone(),
            gates.clone(),
        );
        move |index: usize| {
            let (started, active, peak, notified, gates) = (
                started.clone(),
                active.clone(),
                peak.clone(),
                notified.clone(),
                gates.clone(),
            );
            async move {
                let _guard = Active(active.clone());
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                started.lock().unwrap().push(index);
                notified.notify_one();
                if index != 0 {
                    let _permit = gates[index].acquire().await.unwrap();
                }
                Ok(page(index as u64, "page"))
            }
        }
    });
    let mut reader = pages
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::MIN,
        ))
        .open(vec![0, 2, 1, 2])
        .await
        .unwrap();
    assert_eq!(*started.lock().unwrap(), [0]);
    let reading = tokio::spawn(async move {
        let mut output = Vec::new();
        while let Some(batch) = reader.next().await.unwrap() {
            output.extend_from_slice(
                batch["page"]
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .values(),
            );
        }
        output
    });
    loop {
        let ready = notified.notified();
        if started.lock().unwrap().len() >= 3 {
            break;
        }
        ready.await;
    }
    assert_eq!(started.lock().unwrap().len(), 3);
    assert_eq!(active.load(Ordering::SeqCst), 2);
    gates[1].add_permits(1);
    gates[2].add_permits(2);
    assert_eq!(reading.await.unwrap(), [0, 2, 1, 2]);
    assert!(peak.load(Ordering::SeqCst) <= 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn empty_selections_schema_changes_and_provider_errors_stop_page_reading() {
    let pages = PageSetAccessor::new(|index| async move {
        Ok(page(index, if index == 0 { "page" } else { "changed" }))
    });
    assert_eq!(
        pages.plan_scan(vec![]).await.err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    let mut reader = pages.open(vec![0, 1]).await.unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert_eq!(
        reader.next().await.unwrap_err().operation(),
        "read_page_set"
    );
    assert!(reader.next().await.unwrap().is_none());
    let calls = Arc::new(AtomicUsize::new(0));
    let pages = PageSetAccessor::new({
        let calls = calls.clone();
        move |index| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if index == 1 {
                    return Err(Error::new(
                        ErrorKind::RateLimited,
                        "provider_page",
                        "retry later",
                    ));
                }
                Ok(page(index, "page"))
            }
        }
    });
    let mut reader = pages
        .layer(ParallelReadLayer::new(NonZeroUsize::MIN, NonZeroUsize::MIN))
        .open(vec![0, 1, 2])
        .await
        .unwrap();
    assert!(reader.next().await.unwrap().is_some());
    let error = reader.next().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::RateLimited);
    assert_eq!(error.operation(), "provider_page");
    assert!(reader.next().await.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
