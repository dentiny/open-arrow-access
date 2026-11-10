use std::{num::NonZeroUsize, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::{Accessor, AccessorExt, Bytes, ReadRequest, ScanPlanner};
use open_arrow_access_file_format::{
    json::JsonFormat,
    xml::{XmlField, XmlFormat, XmlSchema},
};
use open_arrow_access_layers::ParallelReadLayer;
use open_arrow_access_service::{common::ApiDocument, oai::OaiPage};
use open_arrow_access_test_support::http::HttpFixture;

#[tokio::test]
async fn selected_json_records_preserve_order_and_links_without_fetching_them() {
    let server = HttpFixture::new();
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
        .plan_scan(ReadRequest::new("api-page"))
        .await
        .unwrap();
    assert_eq!(plan.partitions.size_hint().0, 1);
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
    assert!(server.requests().is_empty());
    assert!(
        document
            .records_at("/missing", JsonFormat::new(schema))
            .is_err()
    );
}

#[tokio::test]
async fn xml_page_adapter_keeps_record_context_and_does_not_follow_cursor() {
    let page = OaiPage {
        bytes: Bytes::from_static(b"<OAI-PMH><ListRecords><record><id>1</id></record><record><id>2</id></record><resumptionToken>next</resumptionToken></ListRecords></OAI-PMH>"),
        record_count: 2, resumption_token: Some("next".into()),
    };
    let records = page.records(XmlFormat::new("/OAI-PMH/ListRecords/record").with_schema(
        XmlSchema::new(vec![XmlField::new("id", "id", DataType::Int64, false)]).unwrap(),
    ));
    let mut reader = records.open(ReadRequest::new("oai-page")).await.unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(page.resumption_token.as_deref(), Some("next"));
    assert!(reader.next().await.unwrap().is_none());
}
