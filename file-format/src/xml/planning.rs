use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, OrderRequirement, ReadRequest, ReaderFactory,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, async_trait, into_async_read,
    record_stream_reader,
};
use xml_arrow::{XmlBatchBuilder, infer_records, read_records};

use super::{XmlAccessor, error};
use crate::scan::chunks::RecordChunks;

#[async_trait]
impl<B> ScanPlanner for XmlAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory();
        let (validation_schema, mut options, explicit_schema) = format.prepare(&request)?;
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let input = into_async_read(input, "read_xml");
        let (schema, records, output_schema) = if let Some(output_schema) = explicit_schema {
            let records = read_records(
                input,
                validation_schema.clone(),
                &format.record_path,
                options.clone(),
            )
            .map_err(error::xml_error)?;
            (validation_schema, records, output_schema)
        } else {
            let (schema, records) = infer_records(
                input,
                &format.record_path,
                &format.bindings(),
                options.clone(),
                format.inference.clone(),
            )
            .await
            .map_err(error::xml_error)?;
            let (output_schema, projection) =
                request.options.resolve_projection(&schema.arrow_schema())?;
            options.projection = projection;
            (schema, records, output_schema)
        };
        let metadata = RecordMetadata {
            schema: output_schema,
            source: Some(request.object),
        };
        let projection = options.projection;
        // One decode partition owns a bounded group of complete records. A task
        // per record would rebuild Arrow arrays and schedule a worker for every row.
        let rows = request.options.batch_size.get();
        let target_bytes = self
            .partition_options
            .target_bytes
            .get()
            .min(options.max_batch_bytes);
        let records = records
            .enumerate()
            .map(|(index, record)| {
                record
                    .map(|record| (index, record))
                    .map_err(error::xml_error)
            })
            .boxed();
        let plan = RecordChunks::new(records).into_plan(
            metadata,
            rows,
            target_bytes,
            |(_, record)| record.xml.len(),
            move |records, metadata| {
                let schema = schema.clone();
                let projection = projection.clone();
                Box::pin(async move {
                    let first = &records[0];
                    let configure_error =
                        |source| error::conversion(source, first.0 as u64 + 1, first.1.byte_offset);
                    let mut builder = match projection {
                        Some(indices) => XmlBatchBuilder::new_projected(schema, rows, &indices),
                        None => XmlBatchBuilder::new(schema, rows),
                    }
                    .map_err(configure_error)?;
                    let mut last_offset = 0;
                    let mut last_index = 0;
                    for (index, record) in records {
                        last_index = index as u64 + 1;
                        last_offset = record.byte_offset;
                        builder
                            .append_xml(&record.xml)
                            .map_err(|source| error::conversion(source, last_index, last_offset))?;
                    }
                    let batch = builder
                        .flush()
                        .map_err(|source| error::conversion(source, last_index, last_offset))?;
                    Ok(Box::new(record_stream_reader(
                        stream::iter(batch.map(Ok)),
                        metadata,
                        None,
                        OrderRequirement::Reorderable,
                    )) as Box<dyn RecordReader>)
                })
            },
        );
        Ok(plan.with_ordering(self.capability().ordering))
    }
}
