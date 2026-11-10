use futures::{StreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, ReaderFactory, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, StreamReader, async_trait,
    into_async_read,
};

use super::{
    WarcArrowAccessor,
    arrow::{append_record, new_batch},
    decode_error,
};

use crate::scan::chunks::RecordChunks;

type RawRecord = (warc::RawRecordHeader, Vec<u8>);

#[async_trait]
impl<B> ScanPlanner for WarcArrowAccessor<B>
where
    B: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let schema = self.binding.factory().prepare(&request)?;
        let metadata = RecordMetadata {
            schema,
            source: Some(request.object.clone()),
        };
        let input = self
            .binding
            .storage()
            .open(ByteReadRequest::from(&request))
            .await?;
        let records = warc::WarcReader::new(into_async_read(input, "read_warc")).iter_raw_records();
        let records = stream::try_unfold(records, |mut records| async move {
            Ok(records
                .next()
                .await
                .transpose()
                .map_err(decode_error)?
                .map(|record| (record, records)))
        })
        .boxed();
        let chunks = RecordChunks::new(records);
        let rows = request.options.batch_size.get();
        let target_bytes = self.partition_options.target_bytes.get();
        let output = metadata.clone();
        let partitions = stream::try_unfold(chunks, move |mut chunks| {
            let metadata = output.clone();
            async move {
                let Some(records) = chunks.next_chunk(rows, target_bytes, record_size).await?
                else {
                    return Ok(None);
                };
                let partition: ScanPartition = Box::pin(async move {
                    let mut batch = new_batch(&metadata.schema)?;
                    let source = metadata.source.as_ref().unwrap().locator();
                    for (headers, body) in records {
                        let record = warc::Record::<warc::EmptyBody>::try_from(headers)
                            .map_err(decode_error)?
                            .add_body(body);
                        append_record(&mut batch, &record, source)?;
                    }
                    let batch = batch.finish()?;
                    Ok(
                        Box::new(StreamReader::new(stream::iter([Ok(batch)]), metadata))
                            as Box<dyn RecordReader>,
                    )
                });
                Ok(Some((partition, chunks)))
            }
        });
        Ok(ScanPlan::from_stream(metadata, partitions))
    }
}

// Approximate serialized size after the parser normalizes header spelling/duplicates.
// Includes all retained headers, not just projected columns; one record may exceed the target.
fn record_size((headers, body): &RawRecord) -> usize {
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
