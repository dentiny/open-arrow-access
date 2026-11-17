use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, Result, ScanStatistics, Statistic,
    StatisticsProvider, async_trait,
};

use super::{
    ParquetAccessor,
    error::parquet_statistics_input_error,
    input::load_metadata,
    validation::{validate_input, validate_statistics_row_count},
};

#[async_trait]
impl<B> StatisticsProvider for ParquetAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    /// Inspect the footer and encoded input length without decoding any rows.
    /// Projection is validated against the footer schema but does not change
    /// row count or encoded source size. Byte read options are forwarded unchanged.
    async fn statistics(&self, request: ReadRequest) -> Result<ScanStatistics> {
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        validate_input(input.as_ref(), parquet_statistics_input_error)?;
        let source_bytes = input
            .metadata()
            .content_length
            .map_or(Statistic::Unknown, Statistic::Exact);
        let metadata = load_metadata(input, "inspect_parquet_statistics").await?;
        request.options.resolve_projection(metadata.schema())?;
        let rows = validate_statistics_row_count(metadata.metadata().file_metadata().num_rows())?;
        Ok(ScanStatistics {
            row_count: Statistic::Exact(rows),
            source_bytes,
        })
    }
}
