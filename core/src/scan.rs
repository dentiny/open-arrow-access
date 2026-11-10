use std::sync::Arc;

use futures::{
    StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{self, BoxStream},
};

use crate::{
    Accessor, RecordMetadata, RecordReader, Result, StreamReader, async_trait, into_stream,
};

use crate::error::scan_schema_mismatch;
/// Semantic permission to relax the logical order of a record scan.
/// Unknown or application-defined sources conservatively preserve global order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OrderRequirement {
    Reorderable,
    #[default]
    PreserveGlobal,
}

impl OrderRequirement {
    /// Composition may strengthen an ordering requirement, never weaken it.
    pub fn combine(self, other: Self) -> Self {
        if self == Self::PreserveGlobal || other == Self::PreserveGlobal {
            Self::PreserveGlobal
        } else {
            Self::Reorderable
        }
    }
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
    pub partitions: BoxStream<'static, Result<ScanPartition>>,
}

impl ScanPlan {
    /// Read partitions sequentially in plan order, opening each on demand.
    /// Validate partition metadata and batches against the fixed scan schema.
    /// Discovery, opening, or reading errors end the reader without opening later partitions.
    pub fn into_reader(self) -> impl RecordReader {
        let schema = self.metadata.schema.clone();
        let batch_schema = schema.clone();
        let batches = self
            .partitions
            .and_then(move |partition| {
                let schema = schema.clone();
                async move {
                    let reader = partition.await?;
                    if reader.metadata().schema != schema {
                        return Err(scan_schema_mismatch());
                    }
                    Ok(into_stream(reader))
                }
            })
            .try_flatten()
            .map(move |result| {
                let batch = result?;
                if batch.schema() != batch_schema {
                    return Err(scan_schema_mismatch());
                }
                Ok(batch)
            });
        StreamReader::new(batches, self.metadata)
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
    /// Requirement covering every selection accepted by this accessor, without I/O.
    /// A mixed/unknown source must retain the conservative default. A resolved plan
    /// may strengthen this declaration for a particular selection.
    fn order_requirement(&self) -> OrderRequirement {
        OrderRequirement::PreserveGlobal
    }

    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan>;
}

#[async_trait]
impl<A: ScanPlanner + ?Sized> ScanPlanner for Arc<A> {
    fn order_requirement(&self) -> OrderRequirement {
        (**self).order_requirement()
    }

    async fn plan_scan(&self, request: Self::Request) -> Result<ScanPlan> {
        (**self).plan_scan(request).await
    }
}
