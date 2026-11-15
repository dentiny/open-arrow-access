use std::num::NonZeroUsize;

use arrow_array::Int64Array;
use futures::TryStreamExt;
use open_arrow_access::{
    OuterCompression, PreparedSource, ReadOptions, ReadRequest,
    file_format::{DecodePartitionOptions, json::JsonFormat},
    file_scan_accessor,
};
use open_arrow_access_test_support::{fixture_path, fs_operator};

#[tokio::test]
async fn prepared_file_retains_native_partitions_and_execution_projection() {
    let accessor = file_scan_accessor(
        fs_operator(fixture_path!("json", "")),
        JsonFormat::infer(),
        OuterCompression::None,
    )
    .with_partition_options(DecodePartitionOptions {
        target_bytes: NonZeroUsize::MIN,
    });
    let source = PreparedSource::prepare_plannable(accessor, ReadRequest::new("projection.jsonl"))
        .await
        .unwrap();
    assert_eq!(source.schema().fields().len(), 2);
    let options = ReadOptions {
        projection: Some(vec!["id".into()]),
        ..Default::default()
    };
    let plan = source.plan_scan(options.clone()).await.unwrap();
    let schema = plan.metadata.schema;
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    // The byte target creates two native JSON partitions, rather than a single
    // whole-input fallback, even though the scan batch size can hold both rows.
    assert_eq!(partitions.len(), 2);
    let mut sequential = source.scan(options).await.unwrap();
    drop(source);

    let mut ids = Vec::new();
    for partition in partitions {
        let mut reader = partition.await.unwrap();
        assert_eq!(reader.metadata().schema, schema);
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), schema);
            ids.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
    }
    assert_eq!(ids, [1, 2]);
    let batch = sequential.next().await.unwrap().unwrap();
    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_rows(), 2);
    assert!(sequential.next().await.unwrap().is_none());
}

#[tokio::test]
#[cfg(feature = "decompression-gzip")]
async fn automatic_decompression_preserves_native_json_partitions() {
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use open_arrow_access::ScanPlanner;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "trajectory_id",
        DataType::Utf8,
        false,
    )]));
    let accessor = file_scan_accessor(
        fs_operator(fixture_path!("atif", "")),
        JsonFormat::new(schema.clone()),
        OuterCompression::Auto,
    )
    .with_partition_options(DecodePartitionOptions {
        target_bytes: NonZeroUsize::MIN,
    });
    let plan = accessor
        .plan_scan(ReadRequest::new("trajectories.jsonl.gz"))
        .await
        .unwrap();
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    assert_eq!(partitions.len(), 3);
    drop(accessor);
    let mut ids = Vec::new();
    for partition in partitions {
        let mut reader = partition.await.unwrap();
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.schema(), schema);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            ids.extend(values.iter().map(|value| value.unwrap().to_owned()));
        }
    }
    assert_eq!(ids, ["first", "second", "third"]);
}

#[tokio::test]
#[cfg(feature = "format-parquet")]
async fn unchanged_bytes_preserve_parquet_row_group_planning() {
    use open_arrow_access::{ScanPlanner, file_format::parquet::ParquetFormat};

    let accessor = file_scan_accessor(
        fs_operator(fixture_path!("parquet", "")),
        ParquetFormat,
        OuterCompression::None,
    );
    let plan = accessor
        .plan_scan(
            ReadRequest::new("ordered-uncompressed.parquet").with_options(ReadOptions {
                projection: Some(vec!["id".into()]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    assert_eq!(partitions.len(), 4);
    drop(accessor);
    let mut ids = Vec::new();
    for partition in partitions {
        let mut reader = partition.await.unwrap();
        while let Some(batch) = reader.next().await.unwrap() {
            assert_eq!(batch.num_columns(), 1);
            ids.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }
    }
    assert_eq!(ids, [0, 1, 2, 3, 4, 5, 6]);
}
