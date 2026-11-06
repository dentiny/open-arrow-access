use arrow_array::LargeBinaryArray;
use open_arrow_access::{
    Accessor,
    service::pmc::{PmcAccessor, PmcRequest},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::test]
async fn discovers_implicit_object_store_prefixes_through_the_public_facade()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let operator = opendal::Operator::new(opendal::services::Memory::default())?;
    // Object stores need no directory marker for metadata/ or PMC123.2/.
    operator
        .write(
            "metadata/a.json",
            include_bytes!("../testdata/pmc/metadata.json").as_slice(),
        )
        .await?;
    let pdf = include_bytes!("../testdata/pmc/article.pdf");
    operator
        .write("PMC123.2/article +%20.pdf", pdf.as_slice())
        .await?;
    let source = PmcAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = source.open(PmcRequest::prefix("metadata/")).await?;
    let batch = reader.next().await?.unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        pdf
    );
    assert!(reader.next().await?.is_none());
    Ok(())
}
