use open_arrow_access::{Accessor, ReadRequest, service::pmc::PmcAccessor};
use open_arrow_access_test_support::MemoryAccessor;
#[tokio::test]
async fn metadata_without_pdf_is_a_row_through_the_facade() {
    let mut reader = PmcAccessor::new(MemoryAccessor::new(
        br#"{"pmcid":"PMC1","license_code":"TDM"}"#,
        1,
    ))
    .open(ReadRequest::new("metadata/a.json"))
    .await
    .unwrap();
    let batch = reader.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column_by_name("pdf_url").unwrap().is_null(0));
    assert!(reader.next().await.unwrap().is_none());
}
