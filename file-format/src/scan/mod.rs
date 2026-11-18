use std::num::NonZeroUsize;

use futures::{FutureExt, StreamExt, TryStreamExt};

use open_arrow_access_core::{
    Accessor, ReadRequest, RecordReader, Result, ScanPlan, ScanPlanner, async_trait,
};

const DEFAULT_TARGET_BYTES: NonZeroUsize = NonZeroUsize::new(4 * 1024 * 1024).unwrap();

/// Soft chunking targets for file-internal decode partitions. Complete records
/// are never rejected for exceeding the target; this is not a memory limit.
#[derive(Clone, Copy, Debug)]
pub struct DecodePartitionOptions {
    /// Target serialized bytes per partition; one larger record may exceed this.
    pub target_bytes: NonZeroUsize,
}

impl Default for DecodePartitionOptions {
    fn default() -> Self {
        Self {
            target_bytes: DEFAULT_TARGET_BYTES,
        }
    }
}

#[allow(dead_code)] // Some builds enable no formats that use record chunking.
pub(crate) mod chunks;

#[cfg(any(feature = "csv", feature = "json"))]
pub(crate) mod byte_chunks;

/// File formats implement planning; the shared accessor attaches input context.
#[async_trait]
pub(crate) trait FileScanPlanner:
    Accessor<Request = ReadRequest, Reader = dyn RecordReader>
{
    async fn plan_file_scan(&self, request: ReadRequest) -> Result<ScanPlan>;
}

#[async_trait]
impl<B, F> ScanPlanner for crate::FileAccessor<B, F>
where
    Self: FileScanPlanner,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let object = request.object.clone();
        let context = move |error: open_arrow_access_core::Error| error.with_object(Some(&object));
        let mut plan = self.plan_file_scan(request).await.map_err(&context)?;
        plan.partitions = plan
            .partitions
            .map_err(context.clone())
            .map_ok(move |partition| {
                let context = context.clone();
                async move { partition.await.map_err(context) }.boxed()
            })
            .boxed();
        Ok(plan)
    }
}
