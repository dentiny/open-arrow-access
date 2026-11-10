use std::{future::Future, marker::PhantomData, sync::Arc};

use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, Capability, RecordMetadata, RecordReader, Result, ScanPartition, ScanPlan,
    ScanPlanner, async_trait,
};

use super::error::empty_page_selection;

/// An ordered selection of independently fetchable API requests.
/// The opener owns provider validation, authentication and record decoding.
/// Cursor-dependent continuations must be discovered before constructing a selection.
/// Install ParallelReadLayer to overlap page fetching and decoding within its budget.
pub struct PageSetAccessor<F, R, Fut> {
    open: Arc<F>,
    request: PhantomData<fn(R) -> Fut>,
}

impl<F, R, Fut> PageSetAccessor<F, R, Fut>
where
    F: Fn(R) -> Fut,
{
    /// Construction does not fetch pages. Request order and duplicates are retained.
    pub fn new(open: F) -> Self {
        Self {
            open: Arc::new(open),
            request: PhantomData,
        }
    }
}

#[async_trait]
impl<F, R, Fut> Accessor for PageSetAccessor<F, R, Fut>
where
    F: Fn(R) -> Fut + Send + Sync + 'static,
    R: Send + 'static,
    Fut: Future<Output = Result<Box<dyn RecordReader>>> + Send + 'static,
{
    type Request = Vec<R>;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, requests: Vec<R>) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(requests).await?.into_reader()))
    }
}

#[async_trait]
impl<F, R, Fut> ScanPlanner for PageSetAccessor<F, R, Fut>
where
    F: Fn(R) -> Fut + Send + Sync + 'static,
    R: Send + 'static,
    Fut: Future<Output = Result<Box<dyn RecordReader>>> + Send + 'static,
{
    async fn plan_scan(&self, requests: Vec<R>) -> Result<ScanPlan> {
        let mut requests = requests.into_iter();
        let first = requests.next().ok_or_else(empty_page_selection)?;
        let first = (self.open)(first).await?;
        let metadata = RecordMetadata {
            schema: first.metadata().schema.clone(),
            source: None,
        };
        let first: ScanPartition = Box::pin(async move { Ok(first) });
        let open = self.open.clone();
        let rest = requests.map(move |request| {
            let open = open.clone();
            let partition: ScanPartition = Box::pin(async move { open(request).await });
            Ok(partition)
        });
        let partitions = stream::iter([Ok(first)]).chain(stream::iter(rest));
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}
