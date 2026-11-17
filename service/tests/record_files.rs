use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::{Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use open_arrow_access_core::{Accessor, AccessorExt, ErrorKind, ReadOptions, Reader, ScanPlanner};
use open_arrow_access_file_format::json::JsonFormat;
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::FileSetRequest;
use open_arrow_access_service::common::{Connection, MetadataLink};
use open_arrow_access_test_support::http::HttpFixture;

#[tokio::test]
async fn resolved_files_keep_membership_order_duplicates_and_lazy_parallel_reads() {
    let server = HttpFixture::new();
    server.file("/a", "{\"id\":1}\n");
    server.file("/b", "{\"id\":2}\n");
    let connection = Connection::new(server.endpoint())
        .unwrap()
        .with_context(server.context());
    let files = connection
        .files(
            vec![
                MetadataLink {
                    file_name: "a.json".into(),
                    url: format!("{}/a", server.endpoint()),
                },
                MetadataLink {
                    file_name: "b.json".into(),
                    url: format!("{}/b", server.endpoint()),
                },
            ],
            (),
        )
        .unwrap();
    let records = files
        .records(JsonFormat::new(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::UInt64,
            false,
        )]))))
        .with_file_planning();
    assert_eq!(
        records
            .plan_scan(FileSetRequest::new(["unknown.json"]))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    assert!(server.requests().is_empty());
    let request = FileSetRequest::new(["b.json", "a.json", "b.json"]);
    let plan = records.plan_scan(request.clone()).await.unwrap();
    // Only the first selected file is opened to fix the schema.
    assert!(!server.requests().iter().any(|request| request.path == "/a"));
    let partitions: Vec<_> = plan.partitions.try_collect().await.unwrap();
    assert_eq!(partitions.len(), 3);
    drop(partitions);
    let mut reader = records
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .open(request)
        .await
        .unwrap();
    let mut values: Vec<u64> = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        values.extend(
            batch["id"]
                .as_primitive::<arrow_array::types::UInt64Type>()
                .values(),
        );
    }
    assert_eq!(values, [2, 1, 2]);
}

#[tokio::test]
async fn selected_attachments_union_schemas_and_restore_projection_order() {
    let server = HttpFixture::new();
    server.file("/a", "{\"id\":1,\"left\":\"A\"}\n");
    server.file("/b", "{\"id\":2,\"right\":\"B\"}\n");
    let files = Connection::new(server.endpoint())
        .unwrap()
        .with_context(server.context())
        .files(
            vec![
                MetadataLink {
                    file_name: "a.json".into(),
                    url: format!("{}/a", server.endpoint()),
                },
                MetadataLink {
                    file_name: "b.json".into(),
                    url: format!("{}/b", server.endpoint()),
                },
            ],
            (),
        )
        .unwrap();
    // Exact schema matching remains the default.
    let mut exact = files
        .clone()
        .records(JsonFormat::infer())
        .open(FileSetRequest::new(["a.json", "b.json"]))
        .await
        .unwrap();
    assert_eq!(exact.next().await.unwrap().unwrap().num_rows(), 1);
    assert_eq!(
        exact.next().await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let records = files
        .records(JsonFormat::infer())
        .with_union_by_name()
        .with_file_planning();
    let request = FileSetRequest::new(["b.json", "a.json", "b.json"]).with_options(ReadOptions {
        projection: Some(vec!["right".into(), "id".into(), "left".into()]),
        ..Default::default()
    });
    let mut reader = records.plan_scan(request).await.unwrap().into_reader();
    let mut rows = Vec::new();
    while let Some(batch) = reader.next().await.unwrap() {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["right", "id", "left"]
        );
        let right = batch["right"].as_string::<i32>();
        let id = batch["id"].as_primitive::<arrow_array::types::Int64Type>();
        let left = batch["left"].as_string::<i32>();
        rows.extend((0..batch.num_rows()).map(|i| {
            (
                (!right.is_null(i)).then(|| right.value(i).to_owned()),
                id.value(i),
                (!left.is_null(i)).then(|| left.value(i).to_owned()),
            )
        }));
    }
    assert_eq!(
        rows,
        [
            (Some("B".into()), 2, None),
            (None, 1, Some("A".into())),
            (Some("B".into()), 2, None)
        ]
    );
}
