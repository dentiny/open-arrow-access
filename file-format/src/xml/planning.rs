use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, ReaderFactory, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_async_read,
    record_stream_reader,
};
use xml_arrow::{XmlBatchBuilder, read_records};

use super::{XmlAccessor, error};

#[async_trait]
impl<B> ScanPlanner for XmlAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let format = self.binding.factory();
        // Inferred mappings are not exposed for reuse across independent workers.
        if format.schema.is_none() {
            return Ok(ScanPlan::whole_input(self.open(request).await?));
        }
        let (schema, options, output_schema) = format.prepare(&request)?;
        let metadata = RecordMetadata {
            schema: output_schema.unwrap(),
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let projection = options.projection.clone();
        let records = read_records(
            into_async_read(input, "read_xml"),
            schema.clone(),
            &format.record_path,
            options,
        )
        .map_err(error::xml_error)?;
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
