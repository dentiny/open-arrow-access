use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
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
        let chunks = WarcChunks {
            records,
            pending: None,
        };
        let rows = request.options.batch_size.get();
        let target_bytes = self.partition_options.target_bytes.get();
        let output = metadata.clone();
        let partitions = stream::try_unfold(chunks, move |mut chunks| {
            let metadata = output.clone();
            async move {
                let Some(records) = chunks.next_chunk(rows, target_bytes).await? else {
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

struct WarcChunks {
    records: BoxStream<'static, Result<RawRecord>>,
    pending: Option<RawRecord>,
}

impl WarcChunks {
    async fn next_chunk(
        &mut self,
        rows: usize,
        target_bytes: usize,
    ) -> Result<Option<Vec<RawRecord>>> {
        let mut chunk = Vec::new();
        let mut bytes = 0usize;
        while chunk.len() < rows {
            let record = match self.pending.take() {
                Some(record) => record,
                None => match self.records.try_next().await? {
                    Some(record) => record,
                    None => break,
                },
            };
            let size = record_size(&record);
            if !chunk.is_empty() && bytes.saturating_add(size) > target_bytes {
                self.pending = Some(record);
                break;
            }
            bytes = bytes.saturating_add(size);
            chunk.push(record);
        }
        Ok((!chunk.is_empty()).then_some(chunk))
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
