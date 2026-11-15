#![cfg(feature = "parquet")]

use open_arrow_access_core::{ReadRequest, Statistic, StatisticsProvider};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture_path};

#[tokio::test]
async fn statistics_report_row_count_and_source_bytes() {
    let data = tokio::fs::read(fixture_path!("parquet", "ordered-uncompressed.parquet"))
        .await
        .unwrap();
    let accessor = ParquetAccessor::new(MemoryAccessor::new(&data, 64).with_ranges());
    let stats = accessor
        .statistics(ReadRequest::new("data.parquet"))
        .await
        .unwrap();

    assert_eq!(stats.row_count, Statistic::Exact(7));
    assert_eq!(stats.source_bytes, Statistic::Exact(data.len() as u64));
}
