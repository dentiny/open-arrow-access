#![cfg(any(
    feature = "parquet-brotli",
    feature = "parquet-gzip",
    feature = "parquet-lz4",
    feature = "parquet-zstd",
))]

use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use more_asserts::{assert_gt, assert_le};
use open_arrow_access_core::{Accessor, AccessorExt, ReadOptions, ReadRequest, into_stream};
use open_arrow_access_file_format::parquet::ParquetAccessor;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_test_support::{MemoryAccessor, fixture};

async fn check_compression(name: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let expected = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..7)),
            Arc::new(StringArray::from(vec![
                Some("zero"),
                None,
                Some("two"),
                Some("three"),
                None,
                Some("five"),
                Some("six"),
            ])),
        ],
    )
    .unwrap();
    let bytes = fixture!("parquet", name).await;
    let accessor = ParquetAccessor::new(MemoryAccessor::new(&bytes, 17).with_ranges());
    let request = ReadRequest::new("data.parquet").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    });
    let sequential = into_stream(accessor.open(request.clone()).await.unwrap())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let parallel = accessor.layer(ParallelReadLayer::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::MIN,
    ));
    let partitioned = into_stream(parallel.open(request).await.unwrap())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    for batches in [sequential, partitioned] {
        let mut offset = 0;
        for batch in batches {
            let rows = batch.num_rows();
            assert_gt!(rows, 0);
            assert_le!(rows, 2);
            assert_eq!(batch, expected.slice(offset, batch.num_rows()));
            offset += batch.num_rows();
        }
        assert_eq!(offset, expected.num_rows());
    }
}

#[cfg(feature = "parquet-brotli")]
#[tokio::test]
async fn brotli_preserves_values_and_parallel_row_groups() {
    check_compression("codec-brotli.parquet").await;
}

#[cfg(feature = "parquet-gzip")]
#[tokio::test]
async fn gzip_preserves_values_and_parallel_row_groups() {
    check_compression("codec-gzip.parquet").await;
}

#[cfg(feature = "parquet-lz4")]
#[tokio::test]
async fn lz4_preserves_values_and_parallel_row_groups() {
    check_compression("codec-lz4.parquet").await;
}

#[cfg(feature = "parquet-zstd")]
#[tokio::test]
async fn zstd_preserves_values_and_parallel_row_groups() {
    check_compression("codec-zstd.parquet").await;
}
