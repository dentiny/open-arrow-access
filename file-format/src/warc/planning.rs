use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, OrderRequirement, ReadRequest, ReaderFactory,
    RecordMetadata, Result, ScanPlan, StreamReader, async_trait,
};

use super::{
    WarcArrowAccessor,
    arrow::{append_record, evolve_reader, new_batch, raw_projection},
    records::{RawRecord, read_records},
};

use crate::scan::chunks::RecordChunks;

#[async_trait]
impl<B> crate::scan::FileScanPlanner for WarcArrowAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_file_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        self.plan_file_scan_with_source_columns(request, &[]).await
    }
    async fn plan_file_scan_with_source_columns(
        &self,
        request: ReadRequest,
        columns: &[open_arrow_access_core::SourceColumn],
    ) -> Result<ScanPlan> {
        let schema = self
            .binding
            .factory()
            .prepare_source_columns(&request, columns)?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let records = read_records(input, columns)?;
        let plan = RecordChunks::new(records).into_plan(
            metadata,
            request.options.batch_size.get(),
            self.partition_options.target_bytes.get(),
            record_size,
            move |records, metadata| {
                Box::pin(async move {
                    let mut batch = new_batch(&metadata.schema)?;
                    let source = metadata.source.as_ref().unwrap().locator();
                    for record in records {
                        append_record(&mut batch, record, source)?;
                    }
                    let batch = batch.finish()?;
                    let metadata = RecordMetadata {
                        schema: raw_projection(&metadata.schema)?,
                        ..metadata
                    };
                    evolve_reader(Box::new(StreamReader::records(
                        stream::iter([Ok(batch)]),
                        metadata,
                        OrderRequirement::Reorderable,
                    )))
                })
            },
        );
        Ok(plan.with_ordering(self.capability().ordering))
    }
}

// Approximate serialized size after the parser normalizes header spelling/duplicates.
// Includes all retained headers, not just projected columns; one record may exceed the target.
fn record_size(RawRecord { headers, body, .. }: &RawRecord) -> usize {
    headers.headers.iter().fold(
        body.len()
            .saturating_add(headers.version.len())
            .saturating_add(13),
        |size, (name, value)| {
            size.saturating_add(name.as_str().len())
                .saturating_add(value.len())
                .saturating_add(4)
        },
    )
}
