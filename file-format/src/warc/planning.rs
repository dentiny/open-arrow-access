use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, OrderRequirement, ReadRequest, ReaderFactory,
    RecordMetadata, Result, ScanPlan, StreamReader, async_trait, into_async_read,
};

use super::{
    WarcArrowAccessor,
    arrow::{append_record, evolve_reader, new_batch, raw_projection},
    decode_error,
};

use crate::scan::chunks::RecordChunks;

type RawRecord = (
    super::arrow::positions::RawRecord,
    Option<open_arrow_access_core::BytePosition>,
);

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
        let track = columns
            .iter()
            .any(|c| *c != open_arrow_access_core::SourceColumn::Object);
        let records = if track {
            super::arrow::positions::positioned_records(input)?
                .map_ok(|(record, position)| (record, Some(position)))
                .boxed()
        } else {
            let records =
                warc::WarcReader::new(into_async_read(input, "read_warc")).iter_raw_records();
            stream::try_unfold(records, |mut records| async move {
                Ok(records
                    .next()
                    .await
                    .transpose()
                    .map_err(decode_error)?
                    .map(|record| (record, records)))
            })
            .map_ok(|record| (record, None))
            .boxed()
        };
        let plan = RecordChunks::new(records).into_plan(
            metadata,
            request.options.batch_size.get(),
            self.partition_options.target_bytes.get(),
            record_size,
            move |records, metadata| {
                Box::pin(async move {
                    let mut batch = new_batch(&metadata.schema)?;
                    let source = metadata.source.as_ref().unwrap().locator();
                    for ((headers, body), position) in records {
                        let record = warc::Record::<warc::EmptyBody>::try_from(headers)
                            .map_err(decode_error)?
                            .add_body(body);
                        append_record(&mut batch, &record, source, position)?;
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
fn record_size(((headers, body), _): &RawRecord) -> usize {
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
