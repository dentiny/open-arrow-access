#![cfg(feature = "parquet")]

use std::{num::NonZeroUsize, sync::atomic::Ordering};

use open_arrow_access_core::{
    ByteReadOptions, ErrorKind, ReadOptions, ReadRequest, Statistic, StatisticsProvider,
};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_test_support::{MemoryAccessor, fixture_path};

#[tokio::test]
async fn footer_counts_rows_and_encoded_bytes_independently_of_projection() {
    let data = tokio::fs::read(fixture_path!("parquet", "codec-zstd.parquet"))
        .await
        .unwrap();
    // Inspecting a compressed file needs no row decoder or parquet-zstd feature.
    let bytes = data.len() as u64;
    let input = MemoryAccessor::new(&data, 64).with_ranges();
    let observed = input.observations.clone();
    let accessor = ParquetAccessor::new(input);
    let byte_options = ByteReadOptions {
        io_concurrency: NonZeroUsize::new(2).unwrap(),
        io_chunk_size: Some(NonZeroUsize::new(512).unwrap()),
    };
    for projection in [None, Some(vec!["text".into()]), Some(vec![])] {
        let request = ReadRequest::new("data.parquet")
            .with_options(ReadOptions {
                projection,
                ..Default::default()
            })
            .with_byte_options(byte_options);
        let stats = accessor.statistics(request).await.unwrap();
        assert_eq!(stats.row_count, Statistic::Exact(7));
        assert_eq!(stats.source_bytes, Statistic::Exact(bytes));
    }
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        assert_eq!(request.object.locator(), "data.parquet");
        assert_eq!(request.options, byte_options);
    }
    assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
    assert!(observed.range_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn statistics_validate_projected_fields_against_footer_schema() {
    let data = tokio::fs::read(fixture_path!("parquet", "ordered-uncompressed.parquet"))
        .await
        .unwrap();
    let accessor = ParquetAccessor::new(MemoryAccessor::new(&data, 64).with_ranges());
    for columns in [vec!["missing".into()], vec!["id".into(), "id".into()]] {
        let request = ReadRequest::new("data.parquet").with_options(ReadOptions {
            projection: Some(columns),
            ..Default::default()
        });
        let error = accessor.statistics(request).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}
