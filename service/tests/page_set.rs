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
    Accessor, AccessorExt, RecordBatch, RecordMetadata, RecordReader, StreamReader,
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
