use std::sync::Arc;

use futures::{
    StreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};

use crate::{Accessor, RecordMetadata, RecordReader, Result, async_trait};

/// A lazy, independently owned partition opener. Partition stream order defines logical order.
/// Each opener must create its own reader/decoder state and preserve order within it.
pub type ScanPartition = BoxFuture<'static, Result<Box<dyn RecordReader>>>;

/// A fixed-schema, ordered scan. Planning may inspect the first input, but must not
/// eagerly open all partitions. Duplicate selections remain distinct partitions.
/// All partition metadata and batches must match `metadata.schema` exactly.
pub struct ScanPlan {
    pub metadata: RecordMetadata,
    pub partitions: BoxStream<'static, Result<ScanPartition>>,
}

impl ScanPlan {
    /// Plan an already known ordered partition list without opening its readers.
    pub fn new(metadata: RecordMetadata, partitions: Vec<ScanPartition>) -> Self {
        Self::from_stream(metadata, stream::iter(partitions.into_iter().map(Ok)))
    }

    /// Discover complete input units incrementally. The executor polls discovery
    /// only while its resident partition window has room.
    pub fn from_stream(
        metadata: RecordMetadata,
        partitions: impl futures::Stream<Item = Result<ScanPartition>> + Send + 'static,
    ) -> Self {
        Self {
            metadata,
            partitions: partitions.boxed(),
        }
    }

    /// Keep a stateful or indivisible input in one partition. The reader's schema
    /// is already fixed; partition execution remains lazy. Outer file-set plans
    /// can still run multiple such inputs concurrently.
    pub fn whole_input(reader: Box<dyn RecordReader>) -> Self {
        Self::new(
            reader.metadata().clone(),
            vec![Box::pin(async move { Ok(reader) })],
        )
    }
}

/// Optional extension for sources that can expose independent record partitions.
/// Normal `Accessor::open` remains the sequential path. A plan's partitions must
/// cover the selection exactly once and be listed in the source's logical order.
#[async_trait]
pub trait ScanPlanner: Accessor<Reader = dyn RecordReader> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan>;
}

#[async_trait]
impl<A: ScanPlanner + ?Sized> ScanPlanner for Arc<A> {
    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        (**self).plan_scan(request).await
    }
}
