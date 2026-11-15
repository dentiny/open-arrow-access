use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, OrderRequirement, ReadRequest, RecordCapability, RecordReader, Result,
    ScanStatistics, Statistic, StatisticsProvider, async_trait,
};

struct UnknownSource;

#[async_trait]
impl Accessor for UnknownSource {
    type Request = ReadRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        RecordCapability::sequential(OrderRequirement::PreserveGlobal)
    }

    async fn open(&self, _: ReadRequest) -> Result<Box<dyn RecordReader>> {
        panic!("default statistics must not open the source")
    }
}

#[async_trait]
impl StatisticsProvider for UnknownSource {}

#[tokio::test]
async fn default_statistics_are_unknown_without_opening_input() {
    let source = Arc::new(UnknownSource);
    let stats = source
        .statistics(ReadRequest::new("selection"))
        .await
        .unwrap();
    assert_eq!(stats, ScanStatistics::default());
    assert_eq!(stats.row_count, Statistic::Unknown);
    assert_eq!(stats.source_bytes, Statistic::Unknown);
    assert_ne!(stats.row_count, Statistic::Exact(0));
}
