use futures::stream;
use open_arrow_access_core::{
    Accessor, Layer, RecordCapability, RecordReader, Result, StreamReader, async_trait,
};

/// Keep at most the first `rows` rows of each opened record scan.
///
/// Place this outside `ParallelReadLayer` or a dataset accessor to apply one
/// global limit across all partitions/files. Place it after `SamplingLayer` to
/// cap the number of sampled rows. This is a prefix limit, not a uniform random
/// sample of a fixed size; unordered input can change which rows are retained.
/// The final batch is sliced without copying arrays. Reaching the limit drops
/// the upstream reader immediately, cancelling any outstanding scan work.
///
/// ```
/// use open_arrow_access_core::{Accessor, AccessorExt, RecordReader};
/// use open_arrow_access_layers::{LimitAccessor, LimitLayer};
///
/// fn first_rows<A: Accessor<Reader = dyn RecordReader>>(source: A) -> LimitAccessor<A> {
///     source.layer(LimitLayer::new(100))
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct LimitLayer {
    rows: u64,
}

impl LimitLayer {
    /// Create a row limit. Zero opens the source for metadata but reads no batches.
    pub fn new(rows: u64) -> Self {
        Self { rows }
    }
}

/// Record accessor decorated by [`LimitLayer`]. Each open gets its own budget.
/// This does not implement `ScanPlanner`: a per-partition limit would not enforce
/// a global row count.
#[derive(Clone, Debug)]
pub struct LimitAccessor<A> {
    inner: A,
    rows: u64,
}

impl<A: Accessor<Reader = dyn RecordReader>> Layer<A> for LimitLayer {
    type Layered = LimitAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        LimitAccessor {
            inner,
            rows: self.rows,
        }
    }
}

#[async_trait]
impl<A: Accessor<Reader = dyn RecordReader>> Accessor for LimitAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let inner = self.inner.open(request).await?;
        let metadata = inner.metadata().clone();
        let capability = inner.capability();
        let inner = (self.rows != 0).then_some(inner);
        let batches = stream::unfold((inner, self.rows), |(inner, remaining)| async move {
            let mut inner = inner?;
            let batch = match inner.next().await {
                Ok(Some(batch)) => batch,
                Ok(None) => return None,
                Err(error) => return Some((Err(error), (None, 0))),
            };
            let rows = remaining.min(batch.num_rows() as u64) as usize;
            let batch = if rows < batch.num_rows() {
                batch.slice(0, rows)
            } else {
                batch
            };
            let remaining = remaining - rows as u64;
            // Release even when the caller never asks for EOF after the last batch.
            let inner = (remaining != 0).then_some(inner);
            Some((Ok(batch), (inner, remaining)))
        });
        Ok(Box::new(StreamReader::records_with_capability(
            batches, metadata, capability,
        )))
    }
}
