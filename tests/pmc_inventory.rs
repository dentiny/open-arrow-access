use open_arrow_access::{Accessor, ReadRequest, service::pmc_inventory::PmcInventoryAccessor};
use open_arrow_access_test_support::MemoryAccessor;
#[tokio::test]
async fn inventory_only_facade_opens_an_empty_explicit_snapshot() {
    let raw=br#"{"sourceBucket":"pmc-oa-opendata","destinationBucket":"arn:aws:s3:::pmc-oa-opendata","version":"2016-11-30","creationTimestamp":"1793581200000","fileFormat":"CSV","fileSchema":"Bucket, Key, LastModifiedDate, ETag","files":[]}"#;
    let mut reader = PmcInventoryAccessor::new(MemoryAccessor::new(raw, 1))
        .open(ReadRequest::new("manifest.json"))
        .await
        .unwrap();
    assert_eq!(reader.metadata().schema.fields().len(), 5);
    assert!(reader.next().await.unwrap().is_none());
}
