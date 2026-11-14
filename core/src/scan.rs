use std::{num::NonZeroUsize, sync::Arc};

use futures::{
    StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};

use crate::{
    Accessor, OrderRequirement, RecordMetadata, RecordReader, Result, StreamReader, async_trait,
    into_stream,
};

/// Requested output policy; permission still comes from `ScanPlan::ordering`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EmissionPolicy {
    #[default]
    Ordered,
    Unordered,
}

/// One scan-wide partition budget, independent of storage byte I/O concurrency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParallelReadOptions {
    /// Maximum resident partitions, including completed queues awaiting consumption.
    pub partitions: NonZeroUsize,
    /// Maximum queued batches per partition; does not bound total bytes.
    pub prefetch: NonZeroUsize,
}

/// A lazy, independently owned partition opener. Partition stream order defines logical order.
/// Each opener must create its own reader/decoder state and preserve order within it.
pub type ScanPartition = BoxFuture<'static, Result<Box<dyn RecordReader>>>;

/// A fixed-schema, ordered scan. Planning may inspect the first input, but must not
/// eagerly open all partitions. Duplicate selections remain distinct partitions.
/// All partition metadata and batches must match `metadata.schema` exactly.
pub struct ScanPlan {
    pub metadata: RecordMetadata,
    /// Resolved requirement for the entire selected input, before execution.
    pub ordering: OrderRequirement,
    /// Execution layers collect settings here before any partition worker starts.
    pub parallel_read: Option<ParallelReadOptions>,
    /// Caller's requested output policy, distinct from the source's `ordering`
    /// requirement. Unordered emission is allowed only for a reorderable source;
    /// a reorderable source may still be read with ordered emission.
    pub emission: EmissionPolicy,
    pub partitions: BoxStream<'static, Result<ScanPartition>>,
}

impl ScanPlan {
    /// Read partitions sequentially in plan order, opening each on demand.
    /// Validate partition metadata and batches against the fixed scan schema.
    /// Discovery, opening, or reading errors end the reader without opening later partitions.
    /// This sequential primitive does not execute collected layer policies; open the
    /// composed accessor to validate and execute those policies.
    pub fn into_reader(self) -> impl RecordReader {
        self.into_reader_with_schema_error(crate::error::scan_schema_mismatch)
    }

    /// Read sequentially like [`Self::into_reader`], using a caller-supplied error
    /// for partition metadata or batch schema mismatches. This lets enclosing
    /// datasets retain their error category and operation context while reusing
    /// scan execution. Discovery, opening and reader errors pass through unchanged.
    pub fn into_reader_with_schema_error(
        self,
        schema_error: fn() -> crate::Error,
    ) -> impl RecordReader {
        let schema = self.metadata.schema.clone();
        let batch_schema = schema.clone();
        let batches = self
            .partitions
            .and_then(move |partition| {
                let schema = schema.clone();
                async move {
                    let reader = partition.await?;
                    crate::validation::validate_scan_schema(
                        &reader.metadata().schema,
                        &schema,
                        schema_error,
                    )?;
                    Ok(into_stream(reader))
                }
            })
            .try_flatten()
            .map(move |result| {
                let batch = result?;
                crate::validation::validate_scan_schema(
                    &batch.schema(),
                    &batch_schema,
                    schema_error,
                )?;
                Ok(batch)
            });
        StreamReader::records(batches, self.metadata, self.ordering)
    }

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
            ordering: OrderRequirement::PreserveGlobal,
            parallel_read: None,
            emission: EmissionPolicy::Ordered,
            partitions: partitions.boxed(),
        }
    }

    /// Declare the resolved source contract. Only a provider that has established
    /// permission for the entire selection may declare `Reorderable`.
    pub fn with_ordering(mut self, ordering: OrderRequirement) -> Self {
        self.ordering = ordering;
        self
    }

    /// Add a surrounding dataset/service requirement without relaxing a child plan.
    pub fn require_ordering(mut self, ordering: OrderRequirement) -> Self {
        self.ordering = self.ordering.combine(ordering);
        self
    }

    /// Keep a stateful or indivisible input in one partition. The reader's schema
    /// is already fixed; partition execution remains lazy. Outer file-set plans
    /// can still run multiple such inputs concurrently.
    pub fn whole_input(reader: Box<dyn RecordReader>) -> Self {
        let ordering = reader.capability().ordering;
        Self::new(
            reader.metadata().clone(),
            vec![Box::pin(async move { Ok(reader) })],
        )
        .with_ordering(ordering)
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
