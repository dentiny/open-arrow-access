use std::sync::Arc;

use arrow_array::{cast::AsArray, types::Int64Type};
use open_arrow_access_core::{Accessor, ReadOptions, ReadRequest, RecordReader, ScanPlanner};
use open_arrow_access_dataset::file_set::{FileSetAccessor, FileSetRequest};
use open_arrow_access_file_format::{csv::CsvAccessor, json::JsonAccessor};
use open_arrow_access_test_support::MemoryAccessor;

/// Yield every open once so concurrency is observable without wall-clock timing.
struct CountedOpens<A> {
    inner: A,
    opens: std::sync::atomic::AtomicUsize,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

#[open_arrow_access_core::async_trait]
impl<A> Accessor for CountedOpens<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> open_arrow_access_core::RecordCapability {
        self.inner.capability()
    }

    async fn open(
        &self,
        request: ReadRequest,
    ) -> open_arrow_access_core::Result<Box<dyn RecordReader>> {
        use std::sync::atomic::Ordering::SeqCst;
        self.opens.fetch_add(1, SeqCst);
        let active = self.active.fetch_add(1, SeqCst) + 1;
        self.peak.fetch_max(active, SeqCst);
        let mut yielded = false;
        futures::future::poll_fn(|cx| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
        let result = self.inner.open(request).await;
        self.active.fetch_sub(1, SeqCst);
        result
    }
}

#[open_arrow_access_core::async_trait]
impl<A> ScanPlanner for CountedOpens<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    async fn plan_scan(
        &self,
        request: ReadRequest,
    ) -> open_arrow_access_core::Result<open_arrow_access_core::ScanPlan> {
        Ok(open_arrow_access_core::ScanPlan::whole_input(
            self.open(request).await?,
        ))
    }
}

#[tokio::test]
async fn union_opens_once_with_bounded_concurrency_and_replays_samples() {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    for json in [false, true] {
        for planned in [false, true] {
            let storage = MemoryAccessor::new(
                if json {
                    b"{\"id\":1}\n".as_slice()
                } else {
                    b"id\n1\n".as_slice()
                },
                3,
            );
            let inner: Arc<dyn Accessor<Request = ReadRequest, Reader = dyn RecordReader>> = if json
            {
                Arc::new(JsonAccessor::infer(storage))
            } else {
                Arc::new(CsvAccessor::infer(storage))
            };
            let source = Arc::new(CountedOpens {
                inner,
                opens: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let mut files = FileSetAccessor::new(source.clone()).with_union_by_name();
            if planned {
                files = files.with_file_planning();
            }
            // Duplicate selections must each retain and replay their own sample.
            let request = FileSetRequest::new(vec!["same"; 40]).with_options(ReadOptions {
                projection: Some(vec!["id".into()]),
                ..Default::default()
            });
            let mut reader = files.open(request).await.unwrap();
            assert_eq!(source.opens.load(SeqCst), 40);
            assert!((2..=16).contains(&source.peak.load(SeqCst)));
            let mut rows = 0;
            while let Some(batch) = reader.next().await.unwrap() {
                rows += batch.num_rows();
                assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 1);
            }
            assert_eq!(rows, 40);
            assert_eq!(source.opens.load(SeqCst), 40);
        }
    }
}
