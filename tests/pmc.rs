use arrow_array::StringArray;
use open_arrow_access::{
    Accessor,
    service::pmc::{PmcAccessor, PmcRequest},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::test]
async fn discovers_implicit_object_store_prefixes_through_the_public_facade()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let operator = opendal::Operator::new(opendal::services::Memory::default())?;
    // The bucket contains metadata only; no PDF object or directory marker exists.
    operator
        .write(
            "metadata/a.json",
            include_bytes!("../testdata/pmc/metadata.json").as_slice(),
        )
        .await?;
    let source = PmcAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = source.open(PmcRequest::prefix("metadata/")).await?;
    let batch = reader.next().await?.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "s3://pmc-oa-opendata/PMC123.2/article%20%2B%2520.pdf?md5=example"
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}
