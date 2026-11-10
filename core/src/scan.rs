use std::sync::Arc;

use futures::future::BoxFuture;

use crate::{Accessor, RecordMetadata, RecordReader, Result, async_trait};

/// A lazy, independently owned partition opener. Vector position defines logical order.
/// Each opener must create its own reader/decoder state and preserve order within it.
pub type ScanPartition = BoxFuture<'static, Result<Box<dyn RecordReader>>>;

/// A fixed-schema, ordered scan. Planning may inspect the first input, but must not
/// eagerly open all partitions. Duplicate selections remain distinct partitions.
/// All partition metadata and batches must match `metadata.schema` exactly.
pub struct ScanPlan {
    pub metadata: RecordMetadata,
    pub partitions: Vec<ScanPartition>,
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
