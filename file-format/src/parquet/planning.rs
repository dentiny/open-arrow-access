use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, RecordMetadata, Result, ScanPartition,
    ScanPlan, ScanPlanner, async_trait,
};
use parquet::arrow::{
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    async_reader::ParquetRecordBatchStreamBuilder,
};

use super::{
    ParquetAccessor, decode_error,
    error::parquet_partition_input_error,
    format::create_reader,
    input::ParquetInput,
    validation::{validate_input, validate_partition_input},
};

#[async_trait]
impl<B> ScanPlanner for ParquetAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + Clone + 'static,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let storage = self.binding.storage().clone();
        let input = storage.open(ByteReadRequest::from(&request)).await?;
        validate_input(input.as_ref(), parquet_partition_input_error)?;
        let identity = input.metadata().clone();
        let metadata = ArrowReaderMetadata::load_async(
            &mut ParquetInput::new(input),
            ArrowReaderOptions::default(),
        )
        .await
        .map_err(|error| decode_error("inspect_parquet", error))?;
        let (schema, _) = request.options.resolve_projection(metadata.schema())?;
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
        Ok(ScanPlan::from_stream(
            output,
            stream::iter(partitions.map(Ok)),
        ))
    }
}
