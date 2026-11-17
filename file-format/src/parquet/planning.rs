use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, RecordMetadata, Result, ScanPartition,
    ScanPlan, async_trait,
};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use super::{
    ParquetAccessor,
    error::parquet_partition_input_error,
    format::create_reader,
    input::{ParquetInput, load_metadata},
    validation::{validate_input, validate_partition_input},
};

#[async_trait]
impl<B> crate::scan::FileScanPlanner for ParquetAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + Clone + 'static,
{
    async fn plan_file_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let storage = self.binding.storage().clone();
        let input = storage.open(ByteReadRequest::from(&request)).await?;
        validate_input(input.as_ref(), parquet_partition_input_error)?;
        let identity = input.metadata().clone();
        let metadata = load_metadata(input, "inspect_parquet").await?;
        let schema = request
            .options
            .resolve_projection(metadata.schema())?
            .schema;
        let output = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        let partitions = (0..metadata.metadata().num_row_groups()).map(move |index| {
            let storage = storage.clone();
            let metadata = metadata.clone();
            let request = request.clone();
            let identity = identity.clone();
            Box::pin(async move {
                let input = storage.open(ByteReadRequest::from(&request)).await?;
                validate_partition_input(input.as_ref(), &identity)?;
                let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    ParquetInput::new(input),
                    metadata,
                )
                .with_row_groups(vec![index]);
                create_reader(builder, request)
            }) as ScanPartition
        });
        Ok(
            ScanPlan::from_stream(output, stream::iter(partitions.map(Ok)))
                .with_ordering(self.capability().ordering),
        )
    }
}
