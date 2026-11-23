use crate::inference::{ResolvedFormat, SchemaInference};

use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, RecordMetadata, Result, ScanPlan,
    async_trait,
};

use super::{CsvAccessor, CsvDialect, framing::CsvFramer};

#[async_trait]
impl<B> crate::scan::FileScanPlanner for CsvAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_file_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory().for_request(&request);
        let Some(dialect) = format.parallel_dialect() else {
            return Ok(ScanPlan::whole_input(self.open(request).await?)
                .with_ordering(self.capability().ordering));
        };
        let prepared = format.prepare_scan(&request)?;
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let Some(ResolvedFormat { format, input }) = format.resolve_schema(input).await? else {
            return Ok(crate::inference::empty_plan(request));
        };
        let prepared = match prepared {
            Some(prepared) => prepared,
            None => format.prepare_scan(&request)?.expect("resolved CSV schema"),
        };
        let dialect = format.parallel_dialect().unwrap_or(dialect);
        let metadata = RecordMetadata {
            schema: prepared.schema,
            source: Some(request.object.clone()),
        };
        // Workers read canonical CSV, regardless of the source dialect.
        let format = format.with_dialect(CsvDialect {
            header: dialect.header,
            validate_header: dialect.validate_header,
            allow_truncated_rows: dialect.allow_truncated_rows,
            ..CsvDialect::default()
        });
        // Initialization runs on first poll, keeping partition discovery lazy.
        let partitions = crate::scan::byte_chunks::byte_chunk_partitions(
            CsvFramer::new(input, dialect),
            format,
            request,
            self.partition_options.target_bytes.get(),
        );
        Ok(ScanPlan::from_stream(metadata, partitions)
            .with_streaming_discovery()
            .with_ordering(self.capability().ordering))
    }
}
