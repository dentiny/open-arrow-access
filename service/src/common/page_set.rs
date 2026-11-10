use std::{future::Future, marker::PhantomData, sync::Arc};

use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, RecordMetadata, RecordReader, Result, ScanPartition,
    ScanPlan, ScanPlanner, StreamReader, async_trait, into_stream,
};

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
        let first = requests.next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "plan_page_set",
                "empty page selection",
            )
        })?;
        let first = (self.open)(first).await?;
        let metadata = RecordMetadata {
            schema: first.metadata().schema.clone(),
            source: None,
        };
        let first = checked_reader(first, &metadata)?;
        let first: ScanPartition = Box::pin(async move { Ok(first) });
        let open = self.open.clone();
        let output = metadata.clone();
        let rest = requests.map(move |request| {
            let open = open.clone();
            let output = output.clone();
            let partition: ScanPartition = Box::pin(async move {
                let reader = open(request).await?;
                checked_reader(reader, &output)
            });
            Ok(partition)
        });
        let partitions = stream::iter([Ok(first)]).chain(stream::iter(rest));
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

fn schema_mismatch() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_page_set",
        "page schema differs from the first selected page",
    )
}

fn checked_reader(
    reader: Box<dyn RecordReader>,
    output: &RecordMetadata,
) -> Result<Box<dyn RecordReader>> {
    if reader.metadata().schema != output.schema {
        return Err(schema_mismatch());
    }
    let metadata = reader.metadata().clone();
    let schema = output.schema.clone();
    let batches = into_stream(reader).map(move |result| {
        let batch = result?;
        if batch.schema() != schema {
            return Err(schema_mismatch());
        }
        Ok(batch)
    });
    Ok(Box::new(StreamReader::new(batches, metadata)))
}
