use std::sync::Arc;

use crate::{Accessor, RecordReader, Result, async_trait};

/// Accuracy of an observed statistic. Unknown is distinct from an exact zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Statistic<T> {
    #[default]
    Unknown,
    /// Exact for the input observed while collecting statistics, not a snapshot.
    Exact(T),
    /// A provider estimate; consumers must not use it as an exact count.
    Estimated(T),
}

/// Optional statistics for a selected record source, without consuming records.
///
/// These values describe the input observed during collection. They do not pin a
/// version or guarantee that a later scan sees the same contents. Providers must
/// leave unavailable values unknown rather than reporting zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanStatistics {
    /// Rows in the selected input, independent of output column projection.
    pub row_count: Statistic<u64>,
    /// Encoded input length as exposed by the source's byte accessor. For raw
    /// storage this is the physical object size, including format metadata and
    /// internal compression. It is not the decoded Arrow memory size or the
    /// bytes fetched by a projected scan. A byte transformation may expose a
    /// different representation from the original storage object.
    pub source_bytes: Statistic<u64>,
}

/// Optional metadata inspection for record accessors.
///
/// Implementing this trait does not change [`Accessor::open`]. The default returns
/// unknown statistics without opening input. Providers may perform metadata I/O
/// and validate the typed request, but must not consume/decode the selected rows
/// merely to count them. Unsupported individual statistics remain unknown.
#[async_trait]
pub trait StatisticsProvider: Accessor<Reader = dyn RecordReader> {
    async fn statistics(&self, _request: Self::Request) -> Result<ScanStatistics> {
        Ok(ScanStatistics::default())
    }
}

#[async_trait]
impl<A: StatisticsProvider + ?Sized> StatisticsProvider for Arc<A> {
    async fn statistics(&self, request: Self::Request) -> Result<ScanStatistics> {
        (**self).statistics(request).await
    }
}
