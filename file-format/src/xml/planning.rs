use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, ReaderFactory, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_async_read,
    record_stream_reader,
};
use xml_arrow::{XmlBatchBuilder, infer_records, read_records};

use super::{XmlAccessor, error};

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
        let output = metadata.clone();
        let partitions = records.enumerate().map(move |(index, record)| {
            let schema = schema.clone();
            let projection = projection.clone();
            let metadata = output.clone();
            record.map_err(error::xml_error).map(move |record| {
                let partition: ScanPartition = Box::pin(async move {
                    let convert_error =
                        |source| error::conversion(source, index as u64 + 1, record.byte_offset);
                    let mut builder = match projection {
                        Some(indices) => {
                            XmlBatchBuilder::new_projected(schema, /*batch_size=*/ 1, &indices)
                        }
                        None => XmlBatchBuilder::new(schema, /*batch_size=*/ 1),
                    }
                    .map_err(convert_error)?;
                    builder.append_xml(&record.xml).map_err(convert_error)?;
                    let batch = builder.flush().map_err(convert_error)?;
                    Ok(Box::new(record_stream_reader(
                        stream::iter(batch.map(Ok)),
                        metadata,
                        None,
                    )) as Box<dyn RecordReader>)
                });
                partition
            })
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}
