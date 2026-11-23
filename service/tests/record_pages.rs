use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, AccessorExt, Bytes, ReadOptions, ReadRequest, ScanPlanner};
use open_arrow_access_file_format::json::JsonFormat;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::common::ApiDocument;

#[tokio::test]
async fn selected_json_records_preserve_order_and_links_without_fetching_them() {
    let document = ApiDocument::from_bytes(Bytes::from_static(
        br#"{
        "results":[{"id":2,"url":"https://example.invalid/paper.pdf"},
                   {"id":1,"url":"https://example.invalid/other.pdf"}],
        "next":"https://example.invalid/cursor"
    }"#,
    ))
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("url", DataType::LargeUtf8, false),
    ]));
    let records = document
        .records_at("/results", JsonFormat::new(schema.clone()))
        .unwrap();
    let plan = records
        .plan_scan(ReadRequest::new("api-page").with_options(ReadOptions {
            batch_size: NonZeroUsize::MIN,
            ..Default::default()
        }))
        .await
        .unwrap();
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    assert_eq!(partitions.len(), 2);
    let mut reader = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(ReadRequest::new("api-page"))
        .await
        .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch["id"]
            .as_primitive::<arrow_array::types::UInt64Type>()
            .values()
            .as_ref(),
        &[2, 1]
    );
    assert_eq!(
        batch["url"].as_string::<i64>().value(0),
        "https://example.invalid/paper.pdf"
    );
    assert!(reader.next().await.unwrap().is_none());
    let native = document
        .records_at("/results", JsonFormat::new(schema.clone()))
        .unwrap()
        .into_file_accessor();
    let mut native_reader = native.open(ReadRequest::new("api-page")).await.unwrap();
    assert_eq!(native_reader.next().await.unwrap().unwrap(), batch);
    assert!(native_reader.next().await.unwrap().is_none());
    assert!(
        document
            .records_at("/missing", JsonFormat::new(schema))
            .is_err()
    );
}

#[tokio::test]
async fn page_binding_keeps_batch_size_independent_and_metadata_reusable() {
    let document = ApiDocument::from_bytes(Bytes::from_static(
        br#"{"items":[{"id":3},{"id":1},{"id":3},{"id":2},{"id":5}],"unknown":{"keep":true}}"#,
    ))
    .unwrap();
    let original = document.value().clone();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let records = document
        .records_at("/items", JsonFormat::new(schema.clone()))
        .unwrap();
    let request = ReadRequest::new("page").with_options(ReadOptions {
        batch_size: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    });
    let mut reader = records.open(request.clone()).await.unwrap();
    let mut ids = Vec::new();
    let mut sizes = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(batch.schema(), schema);
        sizes.push(batch.num_rows());
        ids.extend_from_slice(
            batch["id"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .values(),
        );
    }
    assert_eq!(sizes, [2, 2, 1]);
    assert_eq!(ids, [3, 1, 3, 2, 5]);
    assert_eq!(document.value(), &original);
    let mut reopened = records.open(request).await.unwrap();
    assert_eq!(reopened.next().await.unwrap().unwrap().num_rows(), 2);

    let object = document
        .records_at("/items/0", JsonFormat::new(schema.clone()))
        .unwrap();
    let mut reader = object.open(ReadRequest::new("object")).await.unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().await.unwrap().is_none());

    let error = document
        .records_at("/absent", JsonFormat::new(schema))
        .err()
        .unwrap();
    assert_eq!(error.operation(), "provider_http");
    assert_eq!(
        error.to_string(),
        "provider_http: missing or invalid /absent"
    );
}
